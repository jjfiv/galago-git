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
import java.util.zip.GZIPInputStream;
import org.lemurproject.galago.core.index.GenericIndexReader;
import org.lemurproject.galago.core.index.corpus.SplitIndexReader;
import org.lemurproject.galago.core.index.disk.VocabularyReader;
import org.lemurproject.galago.core.index.disk.VocabularyReader.TermSlot;
import org.lemurproject.galago.core.index.disk.IndexReader;
import org.lemurproject.galago.tupleflow.ExNihiloSource;
import org.lemurproject.galago.tupleflow.FileSource;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Counter;

/**
 * From a set of inputs, splits the input into many DocumentSplit records.
 * This will usually be in a stage by itself at the beginning of a Galago pipeline.
 * This is somewhat similar to FileSource, except that it can autodetect file formats.
 * This splitter can detect ARC, TREC, TRECWEB and corpus files.
 * 
 * @author trevor, sjh, irmarc
 */
@Verified
@OutputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
public class DocumentSource implements ExNihiloSource<DocumentSplit> {

  static String[][] knownExtensions = {
    {"_mbtei.xml.gz", "mbtei"}
  };
  Counter inputCounter, countCounter;
  public Processor<DocumentSplit> processor;
  TupleFlowParameters parameters;
  int fileId = 0;
  int totalFileCount = 0;
  boolean emitSplits;

  public DocumentSource(TupleFlowParameters parameters) {
    this.parameters = parameters;
    inputCounter = parameters.getCounter("Inputs Processed");
    countCounter = parameters.getCounter("Inputs counted");
  }

  public void run() throws IOException {
    // first count the total number of files
    emitSplits = false;
    fileId = 0; // split count
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
        processFile(file);
      }
    }

    // we now have an accurate count of emitted files / splits
    totalFileCount = fileId;
    fileId = 0; // reset to enumerate splits

    // now process each file
    emitSplits = true;
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
        processFile(file);
      }
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
        processFile(file.getAbsolutePath());
      }
    }
  }

  private void processSubCollectionFile(String fileName) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileName))));
    // Look for a fraction and an absolute number of docs
    double pctthreshold = parameters.getJSON().get("pct", 1.0);
    long numdocs = parameters.getJSON().get("numdocs", -1);
    
    System.out.printf("pct: %f, numdocs=%d\n", pctthreshold, numdocs);

    String path;
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
      path = parts[0];

      if (numdocs > 0) {
        if (running <= numdocs) {
          processFile(path);
        } else if (prevRunning < numdocs) {
          splitsize = numdocs - prevRunning;
        }
      } else if (pctthreshold < 1.0) {
        if (frac <= pctthreshold) {
          processFile(path);
        } else if (prevfrac < pctthreshold) {
          // Works out the number of needed docs to meet the proper pct
          splitsize = (long) Math.round((running * (pctthreshold / frac)) - prevRunning);
        }
      } else {
        processFile(path);
      }

      // Delayed processing - somewhere we set the splitsize, and now we create and process the split
      if (splitsize > 0) {
        // First, make sure this file exists. If not, whine about it and move on
        File actual = new File(path);
        if (!actual.exists()) {
          throw new IOException(String.format("File %s was not found. Exiting.\n", path));
        }

        // Now try to detect what kind of file this is:
        boolean isCompressed = (fileName.endsWith(".gz") || fileName.endsWith(".bz2"));
        String fileType = null;

        // We'll try to detect by extension first, so we don't have to open the file
        String extension = getExtension(fileName);
        if (UniversalParser.isParsable(extension)) {
          fileType = extension;
        } else if (IndexReader.isIndexFile(fileName)) {
          // perhaps the user has renamed the corpus index
          fileType = "corpus";
        } else {
          fileType = detectTrecTextOrWeb(fileName);
          // Eventually it'd be nice to do more format detection here.
        }

        if (fileType != null) {
          if (fileType.equals("corpus")) {
            processCorpusFile(fileName, fileType);
          } else {
            DocumentSplit split = new DocumentSplit(fileName, fileType, isCompressed,
                    "subcoll".getBytes(), Utility.compressLong(splitsize), fileId, totalFileCount);
            processSplit(split);
          }
        }
      }
    }
  }

  /**
   * This is a list file, meaning we need to iterate over its contents to
   * retrieve the file list.
   *
   * Assumptions: Each line in this file should be a filename, NOT a directory.
   *              List file is either uncompressed or compressed using gzip ONLY.
   */
  private void processListFile(String fileName) throws IOException {
    BufferedReader br;
    if (fileName.endsWith("gz")) {
      br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileName))));
    } else {
      br = new BufferedReader(new FileReader(fileName));
    }

    while (br.ready()) {
      String entry = br.readLine().trim();
      if (entry.length() == 0) {
        continue;
      }
      processFile(entry);
    }
    br.close();
    return; // No more to do here -- this file is now "processed"
  }

  private void processFile(String fileName) throws IOException {

    // First, make sure this file exists. If not, whine about it and move on
    File actual = new File(fileName);
    if (!actual.exists()) {
      throw new IOException(String.format("File %s was not found. Exiting.\n", fileName));
    }

    // Now try to detect what kind of file this is:
    boolean isCompressed = (fileName.endsWith(".gz") || fileName.endsWith(".bz2"));
    String fileType = null;

    // We'll try to detect by extension first, so we don't have to open the file
    String extension = getExtension(fileName);

    if (extension.equals("list")) {
      processListFile(fileName);
      return; // Considered processed
    }

    if (extension.equals("subcoll")) {
      processSubCollectionFile(fileName);
      return; // Also processed now
    }

    if (UniversalParser.isParsable(extension)) {
      fileType = extension;
    } else if (IndexReader.isIndexFile(fileName)) {
      // perhaps the user has renamed the corpus index
      fileType = "corpus";
    } else {
      fileType = detectTrecTextOrWeb(fileName);
      // Eventually it'd be nice to do more format detection here.
    }

    if (fileType != null) {
      if (fileType.equals("corpus")) {
        processCorpusFile(fileName, fileType);
      } else {
        processSplit(fileName, fileType, isCompressed);
      }
    }
  }

  private void processSplit(String fileName, String fileType, boolean isCompressed) throws IOException {
    DocumentSplit split = new DocumentSplit(fileName, fileType, isCompressed, new byte[0], new byte[0], fileId, totalFileCount);
    processSplit(split);
  }

  private void processSplit(DocumentSplit split) throws IOException {
    fileId++;
    System.out.printf("Processing split (type=%s): %s\n", split.fileType, split.fileName);
    if (emitSplits) {
      processor.process(split);
      if (inputCounter != null) {
        inputCounter.increment();
      }
    } else {
      if (countCounter != null) {
        countCounter.increment();
      }
    }
  }

  private void processCorpusFile(String fileName, String fileType) throws IOException {

    // we want to divde the corpus up into ~100MB chunks
    long chunkSize = 50 * 1024 * 1024;
    long corpusSize = 0L;

    // if we have a corpus folder
    if (SplitIndexReader.isParallelIndex(fileName)) {
      File folder = new File(fileName).getParentFile();
      for (File f : folder.listFiles()) {
        corpusSize += f.length();
      }
    } else { // else must be a corpus file.
      corpusSize = new File(fileName).length();
    }

    GenericIndexReader reader = GenericIndexReader.getIndexReader(fileName);
    VocabularyReader vocabulary = reader.getVocabulary();
    List<TermSlot> slots = vocabulary.getSlots();
    int pieces = Math.max(2, (int) (corpusSize / chunkSize));
    ArrayList<byte[]> keys = new ArrayList<byte[]>();

    for (int i = 1; i < pieces; ++i) {
      float fraction = (float) i / pieces;
      int slot = (int) (fraction * slots.size());
      keys.add(slots.get(slot).termData);
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
        DocumentSplit split = new DocumentSplit(fileName, fileType, false, firstKey, lastKey, fileId, totalFileCount);
        fileId++;
        if (emitSplits) {
          processor.process(split);
          if (inputCounter != null) {
            inputCounter.increment();
          }
        } else {
          if (countCounter != null) {
            countCounter.increment();
          }
        }
      }
    }
  }

  private String getExtension(String fileName) {
    String[] fields = fileName.split("\\.");

    // A filename needs to have a period to have an extension.
    if (fields.length <= 1) {
      return "";
    }

    // There's confusion b/c of the naming scheme for MBTEI - so define
    // a pattern look for that before we do rule-based stuff.
    for (String[] pattern : knownExtensions) {
      if (fileName.contains(pattern[0])) {
        return pattern[1];
      }
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

    // No 'gz' extension, so just return the last part.
    return fields[fields.length - 1];
  }

  // For now we assume <doc> tags, so we read in one doc
  // (i.e. <doc> to </doc>), and look for the following
  // tags: <docno> and (<text> or <html>)
  private String detectTrecTextOrWeb(String fileName) {
    String fileType = null;
    BufferedReader br;
    try {
      br = new BufferedReader(new FileReader(fileName));
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
      if (emitSplits) {
        if (fileType != null) {
          System.out.println(fileName + " detected as " + fileType);
        } else {
          System.out.println("Unable to determine file type of " + fileName);
        }
      }
      return fileType;
    } catch (IOException ioe) {
      ioe.printStackTrace(System.err);
      return null;
    }
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    FileSource.verify(parameters, handler);
  }
}
