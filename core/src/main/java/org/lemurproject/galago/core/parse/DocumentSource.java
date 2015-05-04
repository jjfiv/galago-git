// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.btree.format.BTreeFactory;
import org.lemurproject.galago.core.btree.format.SplitBTreeReader;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.util.DocumentSplitFactory;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.runtime.FileSource;
import org.lemurproject.galago.utility.*;
import org.lemurproject.galago.utility.btree.disk.GalagoBTreeReader;
import org.lemurproject.galago.utility.btree.disk.VocabularyReader;
import org.lemurproject.galago.utility.btree.disk.VocabularyReader.IndexBlockInfo;
import org.lemurproject.galago.utility.compression.VByte;
import org.lemurproject.galago.utility.debug.Counter;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipFile;

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
    DocumentStreamParser.addExternalParsers(parameters.getJSON().get("parser", Parameters.create()));
  }

  @Override
  public void run() throws IOException {
    // splitBuffer stores the full list of documents to emit.
    ArrayList<DocumentSplit> splitBuffer = new ArrayList<>();
    Parameters conf = parameters.getJSON();

    //logger.log(Level.INFO, parameters.getJSON().toString());

    List<String> paths = parameters.getJSON().getAsList("inputPath", String.class);
    for (String input : paths) {
      File inputFile = new File(input);

      if (inputFile.isFile()) {
        splitBuffer.addAll(processFile(inputFile, conf));
      } else if (inputFile.isDirectory()) {
        splitBuffer.addAll(processDirectory(inputFile, conf));
      } else {
        throw new IOException("Couldn't find file/directory: " + input);
      }
    }

    // we now have an accurate count of emitted files / splits
    int totalFileCount = splitBuffer.size();
    int fileId = 0; // reset to enumerate splits

    // now process each file
    for (DocumentSplit split : splitBuffer) {
      inputCounter.increment();
      if(split.fileType == null)
        split.fileType = "";
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
    List<DocumentSplit> splits = new ArrayList<>(subs.length);
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
    // Be smart here, so we delegate to processDirectory as needed.
    if(fp.isDirectory()) {
      return processDirectory(fp, conf);
    }

    String inputPolicy = conf.get("inputPolicy", "require");

    String forceFileType = conf.get("filetype", (String) null);

    ArrayList<DocumentSplit> documents = new ArrayList<>();


    // First, make sure this file exists. If not, whine about it.
    if (!fp.exists()) {
      switch (inputPolicy) {
        case "require":
          throw new IOException(String.format("File %s was not found. Exiting.\n", fp));
        case "warn":
          logger.warning(String.format("File %s was not found. Skipping.\n", fp));
          return Collections.emptyList();
        default:
          throw new IllegalArgumentException("No such inputPolicy=" + inputPolicy);
      }
    }

    // Now try to detect what kind of file this is:
    boolean isCompressed = StreamCreator.isCompressed(fp.getName());
    String fileType = forceFileType;
    String extension = FSUtil.getExtension(fp);

    // don't allow forcing of filetype on zip files;
    // expect that the "force" applies to the inside
    // only process zip files; don't process zip files somebody has re-compressed
    if (!isCompressed && extension.equals("zip")) {
      documents.addAll(processZipFile(fp, conf));
      return documents;
    }

    // don't allow forcing of filetype on list files:
    // expect that the "force" applies to the inside
    if (extension.equals("list")) {
      documents.addAll(processListFile(fp, conf));
      return documents; // now considered processed1
    }

    // We'll try to detect by extension first, so we don't have to open the file
    if (fileType == null) {
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
        fileType = detectTrecTextOrWeb(StreamCreator.openInputStream(fp), fp.getAbsolutePath());
      }
    }

    // Eventually it'd be nice to do more format detection here.

    if (fileType != null) {
      DocumentSplit split = DocumentSplitFactory.file(fp, fileType);
      return Collections.singletonList(split);
    }else {
        logger.warning(String.format("No parser found for file extension: %s.\n", extension));
    }

    return Collections.emptyList();
  }

  public static List<DocumentSplit> processZipFile(File fp, Parameters conf) throws IOException {
    String forceFileType = conf.get("filetype", (String) null);

    ArrayList<DocumentSplit> splits = new ArrayList<>();
    try (ZipFile zipF = ZipUtil.open(fp)) {
      List<String> names = ZipUtil.listZipFile(zipF);
      for (String name : names) {
        String fileType = forceFileType;
        if (fileType == null) {
          File inside = new File(name);
          String extension = FSUtil.getExtension(inside);
          if (DocumentStreamParser.hasParserForExtension(extension)) {
            fileType = extension;
          } else {
            fileType = detectTrecTextOrWeb(ZipUtil.streamZipEntry(zipF, name), fp.getAbsolutePath() + "!" + name);
          }
        }
        DocumentSplit split = DocumentSplitFactory.file(fp);
        split.fileType = fileType;
        split.innerName = name;
        splits.add(split);
      }
    }
    return splits;
  }

  /**
   * This is a list file, meaning we need to iterate over its contents to
   * retrieve the file list.
   *
   * Assumptions: Each line in this file should be a filename, NOT a directory.
   * List file is either uncompressed or compressed using gzip ONLY.
   */
  private static List<DocumentSplit> processListFile(File file, Parameters conf) throws IOException {
    ArrayList<DocumentSplit> splits = new ArrayList<>();

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

    List<DocumentSplit> splits = new ArrayList<>();

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
        String extension = FSUtil.getExtension(file);
        if (forceFileType != null) {
          fileType = forceFileType;
        } else if (DocumentStreamParser.hasParserForExtension(extension)) {
          fileType = extension;
        } else {
          fileType = detectTrecTextOrWeb(StreamCreator.openInputStream(file), file.getAbsolutePath());
          // Eventually it'd be nice to do more format detection here.
        }

        if (fileType != null) {
          DocumentSplit split = DocumentSplitFactory.file(file, fileType);
          split.startKey = ByteUtil.fromString("subcoll");
          split.endKey = VByte.compressLong(splitsize);
          splits.add(split);
        }
      }
    }
    br.close();

    return splits;
  }

  private static List<DocumentSplit> processCorpusFile(File file, Parameters conf) throws IOException {

    // open the corpus
    GalagoBTreeReader reader = BTreeFactory.getBTreeReader(file);

    // we will divide the corpus by vocab blocks
    VocabularyReader vocabulary = reader.getVocabulary();
    List<IndexBlockInfo> slots = vocabulary.getSlots();
    ArrayList<byte[]> keys = new ArrayList<>();

    if(slots.isEmpty()) {
      throw new IllegalArgumentException("Input Corpus file: "+file+" has an empty vocabulary...");
    }

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

    List<DocumentSplit> splits = new ArrayList<>(pieces);
    for (int i = 0; i < pieces; ++i) {
      byte[] firstKey = ByteUtil.EmptyArr;
      byte[] lastKey = ByteUtil.EmptyArr;

      if (i > 0) {
        firstKey = keys.get(i - 1);
      }
      if (i < pieces - 1) {
        lastKey = keys.get(i);
      }

      if (!CmpUtil.equals(firstKey, lastKey)) {
        DocumentSplit split = DocumentSplitFactory.file(file);
        split.fileType = "corpus";
        split.startKey = firstKey;
        split.endKey = lastKey;
        splits.add(split);
      }
    }
    reader.close();

    return splits;
  }

  // For now we assume <doc> tags, so we read in one doc
  // (i.e. <doc> to </doc>), and look for the following
  // tags: <docno> and (<text> or <html>)
  private static String detectTrecTextOrWeb(InputStream is, String path) throws IOException {
    String fileType = null;
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(is));
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
        System.out.println(path + " detected as " + fileType);
      } else {
        System.out.println("Unable to determine file type of " + path);
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
