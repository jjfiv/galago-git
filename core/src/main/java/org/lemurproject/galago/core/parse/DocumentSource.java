// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.lemurproject.galago.core.index.BTreeFactory;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.corpus.SplitBTreeReader;
import org.lemurproject.galago.core.index.disk.VocabularyReader;
import org.lemurproject.galago.core.index.disk.VocabularyReader.IndexBlockInfo;
import org.lemurproject.galago.tupleflow.ExNihiloSource;
import org.lemurproject.galago.tupleflow.FileSource;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Counter;

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

  static String[][] specialKnownExtensions = {
    {"_mbtei.xml.gz", "mbtei"}
  };
  private Counter inputCounter;
  public Processor<DocumentSplit> processor;
  private TupleFlowParameters parameters;
  private int fileId = 0;
  private int totalFileCount = 0;
  private List<DocumentSplit> splitBuffer;
  private Set<String> externalFileTypes;
  private String dictatedFileType;
  private Logger logger;
  private String inputPolicy;

  public DocumentSource(TupleFlowParameters parameters) {
    this.parameters = parameters;
    inputPolicy = parameters.getJSON().get("inputPolicy", "require");
    this.inputCounter = parameters.getCounter("Inputs Processed");
    logger = Logger.getLogger("DOCSOURCE");
    externalFileTypes = new HashSet<String>();
    dictatedFileType = parameters.getJSON().get("filetype", (String)null);
    if (parameters.getJSON().containsKey("externalParsers")) {
	List<Parameters> extP = parameters.getJSON().getAsList("externalParsers");
	for (Parameters p : extP) {
	    logger.info(String.format("Adding external file type %s\n", 
				      p.getString("filetype")));
	    externalFileTypes.add(p.getString("filetype"));
	}
    }
  }

  public void run() throws IOException {
    // splitBuffer stores the full list of documents to emit.
    splitBuffer = new ArrayList();

    if (parameters.getJSON().containsKey("directory")) {
      List<String> directories = parameters.getJSON().getAsList("directory");
      for (String directory : directories) {
        File directoryFile = new File(directory);
        processDirectory(directoryFile);
      }
    }
    if (parameters.getJSON().containsKey("filename")) {
      List<String> files = parameters.getJSON().getAsList("filename");
      for (String file : files) {
        processFile(new File(file));
      }
    }

    // we now have an accurate count of emitted files / splits
    totalFileCount = splitBuffer.size();
    fileId = 0; // reset to enumerate splits

    // now process each file
    for (DocumentSplit split : splitBuffer) {
      inputCounter.increment();
      split.fileId = fileId;
      split.totalFileCount = totalFileCount;
      processor.process(split);
      fileId++;
    }
    processor.close();
  }

  /// PRIVATE FUNCTIONS ///
  private void processDirectory(File root) throws IOException {
    for (File file : root.listFiles()) {
      if (file.isHidden()) {
        continue;
      }
      if (file.isDirectory()) {
        processDirectory(file);
      } else {
        processFile(file);
      }
    }
  }

  private void processFile(File file) throws IOException {

    // First, make sure this file exists. If not, whine about it.
    if (!file.exists()) {
	if (inputPolicy.equals("require")) {
	    throw new IOException(String.format("File %s was not found. Exiting.\n", file));
	} else if (inputPolicy.equals("warn")) {
	    logger.warning(String.format("File %s was not found. Skipping.\n", file));
	    return;
	} else {
	    // Return quietly
	    return;
	}
    }

    // Now try to detect what kind of file this is:
    boolean isCompressed = (file.getName().endsWith(".gz") || file.getName().endsWith(".bz2"));
    String fileType = null;

    // We'll try to detect by extension first, so we don't have to open the file
    String extension = getExtension(file);

    // first lets look for special cases that require some processing here:
    if (extension.equals("list")) {
      processListFile(file);
      return; // now considered processed
    }

    if (extension.equals("subcoll")) {
      processSubCollectionFile(file);
      return; // now considered processed
    }

    if (dictatedFileType != null) {
	fileType = dictatedFileType;
    } else if (UniversalParser.isParsable(extension) || isExternallyDefined(extension)) {
      fileType = extension;
    } else if (file.getName().equals("corpus") || (BTreeFactory.isBTree(file))) {
      // perhaps the user has renamed the corpus index
      processCorpusFile(file);
      return; // done now;

    } else {
      fileType = detectTrecTextOrWeb(file);
    }
    // Eventually it'd be nice to do more format detection here.

    if (fileType != null) {
      DocumentSplit split = new DocumentSplit(file.getAbsolutePath(), fileType, isCompressed, new byte[0], new byte[0], fileId, totalFileCount);
      this.splitBuffer.add(split);
    }
  }

  /**
   * This is a list file, meaning we need to iterate over its contents to
   * retrieve the file list.
   *
   * Assumptions: Each line in this file should be a filename, NOT a directory.
   * List file is either uncompressed or compressed using gzip ONLY.
   */
  private void processListFile(File file) throws IOException {
    BufferedReader br;
    if (file.getName().endsWith("gz")) {
      br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
    } else {
      br = new BufferedReader(new FileReader(file));
    }

    while (br.ready()) {
      String entry = br.readLine().trim();
      if (entry.length() == 0) {
        continue;
      }

      processFile(new File(entry));
    }
    br.close();
    // No more to do here -- this file is now "processed"
  }

  private void processSubCollectionFile(File file) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
    // Look for a fraction and an absolute number of docs
    double pctthreshold = parameters.getJSON().get("pct", 1.0);
    long numdocs = parameters.getJSON().get("numdocs", -1);

    System.out.printf("pct: %f, numdocs=%d\n", pctthreshold, numdocs);

    File f;
    long prevRunning, running;
    double prevfrac, frac;

    running = 0;
    frac = 0.0;
    long splitsize = 0;


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
          processFile(f);
        } else if (prevRunning < numdocs) {
          splitsize = numdocs - prevRunning;
        }
      } else if (pctthreshold < 1.0) {
        if (frac <= pctthreshold) {
          processFile(f);
        } else if (prevfrac < pctthreshold) {
          // Works out the number of needed docs to meet the proper pct
          splitsize = (long) Math.round((running * (pctthreshold / frac)) - prevRunning);
        }
      } else {
        processFile(f);
      }

      // Delayed processing - somewhere we set the splitsize, and now we create and process the split
      if (splitsize > 0) {
        // First, make sure this file exists. If not, whine about it and move on
        if (!f.exists()) {
          throw new IOException(String.format("File %s was not found. Exiting.\n", f));
        }

        // Now try to detect what kind of file this is:
        boolean isCompressed = (file.getName().endsWith(".gz") || file.getName().endsWith(".bz2"));
        String fileType = null;

        // We'll try to detect by extension first, so we don't have to open the file
        String extension = getExtension(file);
	if (dictatedFileType != null) {
	    fileType = dictatedFileType;
	} else if (UniversalParser.isParsable(extension) || isExternallyDefined(extension)) {
	  fileType = extension;
        } else {
          fileType = detectTrecTextOrWeb(file);
          // Eventually it'd be nice to do more format detection here.
        }

        if (fileType != null) {
          DocumentSplit split = new DocumentSplit(file.getAbsolutePath(), fileType, isCompressed,
                  "subcoll".getBytes(), Utility.compressLong(splitsize), fileId, totalFileCount);
          this.splitBuffer.add(split);
        }
      }
    }
  }

  private void processCorpusFile(File file) throws IOException {

    // open the corpus
    BTreeReader reader = BTreeFactory.getBTreeReader(file);

    // we will divide the corpus by vocab blocks
    VocabularyReader vocabulary = reader.getVocabulary();
    List<IndexBlockInfo> slots = vocabulary.getSlots();
    ArrayList<byte[]> keys = new ArrayList<byte[]>();

    // look for a manually specified number of corpus pieces:
    long pieces = this.parameters.getJSON().get("corpusPieces", 10);

    // otherwise we want to divde the corpus up into ~50MB chunks
    if (pieces < 0) {
      long chunkSize = 50 * 1024 * 1024;
      long corpusSize = 0L;

      // if we have a corpus folder sum the lengths of files in the folder
      if (SplitBTreeReader.isBTree(file)) {
        File folder = file.getParentFile();
        for (File f : folder.listFiles()) {
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
        DocumentSplit split = new DocumentSplit(file.getAbsolutePath(), "corpus", false, firstKey, lastKey, fileId, totalFileCount);
        this.splitBuffer.add(split);
      }
    }
  }

  private String getExtension(File file) {

    String fileName = file.getName();

    // There's confusion b/c of the naming scheme for MBTEI - so define
    // a pattern look for that before we do rule-based stuff.
    for (String[] pattern : specialKnownExtensions) {
      if (fileName.contains(pattern[0])) {
        return pattern[1];
      }
    }

    // now split the filename on '.'s
    String[] fields = fileName.split("\\.");

    // A filename needs to have a period to have an extension.
    if (fields.length <= 1) {
      return "";
    }

    // If the last chunk of the filename is gz, we'll ignore it.
    // The second-to-last bit is the type extension (but only if
    // there are at least three parts to the name).
    if (fields[fields.length - 1].equals("gz")) {
      if (fields.length > 2) {
        return fields[fields.length - 2];
      } else {
        return "";
      }
    }

    // Do the same thing w/ bz2 as above (MAC)
    if (fields[fields.length - 1].equals("bz2")) {
      if (fields.length > 2) {
        return fields[fields.length - 2];
      } else {
        return "";
      }
    }

    // No 'gz'/'bz2' extensions, so just return the last part.
    return fields[fields.length - 1];
  }

  // For now we assume <doc> tags, so we read in one doc
  // (i.e. <doc> to </doc>), and look for the following
  // tags: <docno> and (<text> or <html>)
  private String detectTrecTextOrWeb(File file) {
    String fileType = null;
    BufferedReader br;
    try {
      br = new BufferedReader(new FileReader(file));
      String line;

      // check the first line for a "<doc>" line
      line = br.readLine();
      if (line == null || line.equalsIgnoreCase("<doc>") == false) {
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
        if (line.indexOf("<docno>") != -1) {
          hasDocno = true;
        } else if (line.indexOf("<dochdr>") != -1) {
          hasDocHdr = true;
        } else if (line.indexOf("<text>") != -1) {
          hasText = true;
        } else if (line.indexOf("<html>") != -1) {
          hasHtml = true;
        } else if (line.indexOf("<body>") != -1) {
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
    }
  }

  protected boolean isExternallyDefined(String extension) {
      return externalFileTypes.contains(extension);
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    FileSource.verify(parameters, handler);
  }
}
