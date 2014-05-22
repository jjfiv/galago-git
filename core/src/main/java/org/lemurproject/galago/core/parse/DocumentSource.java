// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.index.BTreeFactory;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.corpus.SplitBTreeReader;
import org.lemurproject.galago.core.index.disk.VocabularyReader;
import org.lemurproject.galago.core.index.disk.VocabularyReader.IndexBlockInfo;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verified;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * From a set of inputs, splits the input into many DocumentSplit records. This
 * will usually be in a stage by itself at the beginning of a Galago pipeline.
 * This is somewhat similar to FileSource, except that it can autodetect file
 * formats. This splitter can detect ARC, TREC, TRECWEB and corpus files.
 *
 * @author trevor, sjh, irmarc
 */
@Verified
@OutputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
public class DocumentSource implements ExNihiloSource<DocumentSplit> {

  private static final Logger logger = Logger.getLogger("DOCSOURCE");

  private Counter inputCounter;
  public Processor<DocumentSplit> processor;
  private TupleFlowParameters parameters;

  public DocumentSource(TupleFlowParameters parameters) {
    this.parameters = parameters;
    this.inputCounter = parameters.getCounter("Inputs Processed");
    DocumentStreamParser.addExternalParsers(parameters.getJSON().get("parser", new Parameters()));
  }

  @Override
  public void run() throws IOException {
    // splitBuffer stores the full list of documents to emit.
    ArrayList<DocumentSplit> splitBuffer = new ArrayList<DocumentSplit>();
    Parameters conf = parameters.getJSON();

    logger.log(Level.INFO, parameters.getJSON().toString());

    if (conf.containsKey("directory")) {
      List<String> directories = parameters.getJSON().getAsList("directory", String.class);
      for (String directory : directories) {
        File directoryFile = new File(directory);
        splitBuffer.addAll(processDirectory(directoryFile, conf));
      }
    }
    if (conf.containsKey("filename")) {
      List<String> files = parameters.getJSON().getAsList("filename", String.class);
      for (String file : files) {
        splitBuffer.addAll(processFile(new File(file), conf));
      }
    }

    // we now have an accurate count of emitted files / splits
    int totalFileCount = splitBuffer.size();
    int fileId = 0; // reset to enumerate splits

    // now process each file
    for (DocumentSplit split : splitBuffer) {
      if (inputCounter != null) {
        inputCounter.increment();
      }
      split.fileId = fileId;
      split.totalFileCount = totalFileCount;
      processor.process(split);
      fileId++;
    }
    processor.close();
  }

  public static List<DocumentSplit> processDirectory(File root, Parameters conf) throws IOException {
    System.out.println("Processing directory: " + root);
    
    // add detection of likely corpus folders
    if(root.getName().endsWith("corpus")) {
      if(SplitBTreeReader.isBTree(root)) {
        System.out.println(" * Treating as a corpus (this will skip any other files that happen to be in that directory)");
        return processCorpusFile(root, conf);
        // don't process children, they're part of the corpus
      }
    }

    File[] subs = FileUtility.safeListFiles(root);
    List<DocumentSplit> splits = new ArrayList<DocumentSplit>(subs.length);
    for (File file : subs) {
      if (file.isHidden()) {
        continue;
      }
      if (file.isDirectory()) {
        splits.addAll(processDirectory(file, conf));
      } else {
        splits.addAll(processFile(file, conf));
      }
    }

    return splits;
  }

  public static List<DocumentSplit> processFile(File fp, Parameters conf) throws IOException {
    String inputPolicy = conf.get("inputPolicy", "require");

    String forceFileType = null;
    if(conf.containsKey("filetype"))
      forceFileType = conf.getAsString("filetype");

    ArrayList<DocumentSplit> documents = new ArrayList<DocumentSplit>();

    // First, make sure this file exists. If not, whine about it.
    if (!fp.exists()) {
      if (inputPolicy.equals("require")) {
        throw new IOException(String.format("File %s was not found. Exiting.\n", fp));
      } else if (inputPolicy.equals("warn")) {
        logger.warning(String.format("File %s was not found. Skipping.\n", fp));
        return Collections.emptyList();
      } else {
        throw new IllegalArgumentException("No such inputPolicy="+inputPolicy);
      }
    }

    // Now try to detect what kind of file this is:
    boolean isCompressed = StreamCreator.isCompressed(fp.getName());
    String fileType = forceFileType;

    // We'll try to detect by extension first, so we don't have to open the file
    String extension;
    if (fileType == null) {
      extension = FileUtility.getExtension(fp);

      // first lets look for special cases that require some processing here:
      if (extension.equals("list")) {
        documents.addAll(processListFile(fp, conf));
        return documents; // now considered processed1
      }

      if (extension.equals("subcoll")) {
        documents.addAll(processSubCollectionFile(fp, conf));
        return documents; // now considered processed
      }

      if (DocumentStreamParser.hasParserForExtension(extension)) {
        fileType = extension;

      } else if (!isCompressed && (fp.getName().equals("corpus") || (BTreeFactory.isBTree(fp)))) {
        // perhaps the user has renamed the corpus index, but not if they compressed it
        // we need random access and even bz2 is dumb. just (b|g)?unzip it.
        documents.addAll(processCorpusFile(fp, conf));
        return documents; // done now;

      } else {
        // finally try to be 'clever'...
        fileType = detectTrecTextOrWeb(fp);
      }
    }

    // Eventually it'd be nice to do more format detection here.

    if (fileType != null) {
      DocumentSplit split = new DocumentSplit(fp.getAbsolutePath(), fileType, new byte[0], new byte[0], 0, 0);
      return Collections.singletonList(split);
    }

    return Collections.emptyList();
  }

  /**
   * This is a list file, meaning we need to iterate over its contents to
   * retrieve the file list.
   *
   * Assumptions: Each line in this file should be a filename, NOT a directory.
   * List file is either uncompressed or compressed using gzip ONLY.
   */
  private static List<DocumentSplit> processListFile(File file, Parameters conf) throws IOException {
    ArrayList<DocumentSplit> splits = new ArrayList<DocumentSplit>();

    BufferedReader br;
    if (file.getName().endsWith("gz")) {
      br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
    } else {
      br = new BufferedReader(new FileReader(file));
    }

    int n = 0;
    while (br.ready()) {
      String entry = br.readLine().trim();
      if (entry.length() == 0) {
        continue;
      }
      if(n % 1000 == 0) {
        logger.log(Level.INFO, "considered {0} items from {1}", new Object[]{n, file});
      }
      splits.addAll(processFile(new File(entry), conf));
      n++;
    }
    br.close();
    // No more to do here -- this file is now "processed"
    return splits;
  }

  private static List<DocumentSplit> processSubCollectionFile(File file, Parameters conf) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
    // Look for a fraction and an absolute number of docs
    double pctthreshold = conf.get("pct", 1.0);
    long numdocs = conf.get("numdocs", -1);

    System.out.printf("pct: %f, numdocs=%d\n", pctthreshold, numdocs);

    File f;
    long prevRunning, running;
    double prevfrac, frac;

    running = 0;
    frac = 0.0;
    long splitsize;

    List<DocumentSplit> splits = new ArrayList<DocumentSplit>();

    // Favor the absolute # variable (second one)
    while (br.ready()) {
      splitsize = 0;
      String[] parts = br.readLine().split(":");
      prevRunning = running;
      running = Long.parseLong(parts[1]);
      prevfrac = frac;
      frac = Double.parseDouble(parts[2]);
      f = new File(parts[0]);

      if (numdocs > 0) {
        if (running <= numdocs) {
          splits.addAll(processFile(f, conf));
        } else if (prevRunning < numdocs) {
          splitsize = numdocs - prevRunning;
        }
      } else if (pctthreshold < 1.0) {
        if (frac <= pctthreshold) {
          splits.addAll(processFile(f, conf));
        } else if (prevfrac < pctthreshold) {
          // Works out the number of needed docs to meet the proper pct
          splitsize = Math.round((running * (pctthreshold / frac)) - prevRunning);
        }
      } else {
        splits.addAll(processFile(f, conf));
      }

      // Delayed processing - somewhere we set the splitsize, and now we create and process the split
      if (splitsize > 0) {
        // First, make sure this file exists. If not, whine about it and move on
        if (!f.exists()) {
          throw new IOException(String.format("File %s was not found. Exiting.\n", f));
        }


        // Now try to detect what kind of file this is:
        String forceFileType = conf.get("filetype", (String) null);
        String fileType;
        // We'll try to detect by extension first, so we don't have to open the file
        String extension = FileUtility.getExtension(file);
        if (forceFileType != null) {
          fileType = forceFileType;
        } else if (DocumentStreamParser.hasParserForExtension(extension)) {
          fileType = extension;
        } else {
          fileType = detectTrecTextOrWeb(file);
          // Eventually it'd be nice to do more format detection here.
        }

        if (fileType != null) {
          DocumentSplit split = new DocumentSplit(file.getAbsolutePath(), fileType, "subcoll".getBytes(), Utility.compressLong(splitsize), 0, 0);
          splits.add(split);
        }
      }
    }
    br.close();

    return splits;
  }

  private static List<DocumentSplit> processCorpusFile(File file, Parameters conf) throws IOException {

    // open the corpus
    BTreeReader reader = BTreeFactory.getBTreeReader(file);

    // we will divide the corpus by vocab blocks
    VocabularyReader vocabulary = reader.getVocabulary();
    List<IndexBlockInfo> slots = vocabulary.getSlots();
    ArrayList<byte[]> keys = new ArrayList<byte[]>();

    // look for a manually specified number of corpus pieces:
    int pieces = (int) conf.get("corpusPieces", 10);

    // otherwise we want to divde the corpus up into ~50MB chunks
    if (pieces < 0) {
      long chunkSize = 50 * 1024 * 1024;
      long corpusSize = 0L;

      // if we have a corpus folder sum the lengths of files in the folder
      if (SplitBTreeReader.isBTree(file)) {
        File folder = file.getParentFile();
        for (File f : FileUtility.safeListFiles(folder)) {
          corpusSize += f.length();
        }

      } else {
        // else must be a corpus file.
        corpusSize = file.length();
      }

      pieces = (int) (corpusSize / chunkSize);
    }

    // otherwise we must always emit at least 2 pieces.
    pieces = Math.max(2, pieces);

    logger.info("Splitting corpus into " + pieces);

    for (int i = 1; i < pieces; ++i) {
      float fraction = (float) i / pieces;
      int slot = (int) (fraction * slots.size());
      keys.add(slots.get(slot).firstKey);
    }

    List<DocumentSplit> splits = new ArrayList<DocumentSplit>(pieces);
    for (int i = 0; i < pieces; ++i) {
      byte[] firstKey = new byte[0];
      byte[] lastKey = new byte[0];

      if (i > 0) {
        firstKey = keys.get(i - 1);
      }
      if (i < pieces - 1) {
        lastKey = keys.get(i);
      }

      if (Utility.compare(firstKey, lastKey) != 0) {
        DocumentSplit split = new DocumentSplit(file.getAbsolutePath(), "corpus", firstKey, lastKey, 0, 0);
        splits.add(split);
      }
    }
    reader.close();

    return splits;
  }

  // For now we assume <doc> tags, so we read in one doc
  // (i.e. <doc> to </doc>), and look for the following
  // tags: <docno> and (<text> or <html>)
  private static String detectTrecTextOrWeb(File file) throws IOException {
    String fileType = null;
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(file));
      String line;

      // check the first line for a "<doc>" line
      line = br.readLine();
      if (line == null || !line.equalsIgnoreCase("<doc>")) {
        return fileType;
      }

      // Now just read until we see docno and (text or html) tags
      boolean hasDocno, hasDocHdr, hasHtml, hasText, hasBody;
      hasDocno = hasDocHdr = hasHtml = hasText = hasBody = false;
      while (br.ready()) {
        line = br.readLine();
        if (line == null || line.equalsIgnoreCase("</doc>")) {
          break; // doc is closed or null line
        }
        line = line.toLowerCase();
        if (line.contains("<docno>")) {
          hasDocno = true;
        } else if (line.contains("<dochdr>")) {
          hasDocHdr = true;
        } else if (line.contains("<text>")) {
          hasText = true;
        } else if (line.contains("<html>")) {
          hasHtml = true;
        } else if (line.contains("<body>")) {
          hasBody = true;
        }

        if (hasDocno && hasText) {
          fileType = "trectext";
          break;
        } else if (hasDocno && (hasHtml || hasBody || hasDocHdr)) {
          fileType = "trecweb";
        }
      }
      br.close();
      if (fileType != null) {
        System.out.println(file.getAbsolutePath() + " detected as " + fileType);
      } else {
        System.out.println("Unable to determine file type of " + file.getAbsolutePath());
      }
      return fileType;
    } catch (IOException ioe) {
      ioe.printStackTrace(System.err);
      return null;
    } finally {
      if (br != null) {
        br.close();
      }
    }
  }

  @Override
  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    FileSource.verify(parameters, store);
  }
}
