// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.Verified;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Determines the class type of the input split, either based
 * on the "filetype" parameter passed in, or by guessing based on
 * the file path extension.
 *
 * (7/29/2012, irmarc): Refactored to be plug-and-play. External filetypes
 * may be added via the parameters.
 *
 * @author trevor, sjh, irmarc
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class UniversalParser extends StandardStep<DocumentSplit, Document> {

  // The built-in type map
  static String[][] sFileTypeLookup = {
    {"html", FileParser.class.getName()},
    {"xml", FileParser.class.getName()},
    {"txt", FileParser.class.getName()},
    {"arc", ArcParser.class.getName()},
    {"warc", WARCParser.class.getName()},
    {"trectext", TrecTextParser.class.getName()},
    {"trecweb", TrecWebParser.class.getName()},
    {"twitter", TwitterParser.class.getName()},
    {"corpus", CorpusSplitParser.class.getName()},
    {"selectivecorpus", CorpusSelectiveSplitParser.class.getName()},
    {"wiki", WikiParser.class.getName()}
  };
  private HashMap<String, Class> fileTypeMap;
  private Counter documentCounter;
  private Parameters parameters;
  private static final Logger LOG = Logger.getLogger(UniversalParser.class.getSimpleName());
  private byte[] subCollCheck = "subcoll".getBytes();

  public UniversalParser(TupleFlowParameters parameters) {
    documentCounter = parameters.getCounter("Documents Parsed");
    this.parameters = parameters.getJSON();
    buildFileTypeMap();
  }

  private void buildFileTypeMap() {
    try {
      fileTypeMap = new HashMap<String, Class>();
      for (String[] mapping : sFileTypeLookup) {
        fileTypeMap.put(mapping[0], Class.forName(mapping[1]));
      }

      // Look for external mapping definitions
      if (parameters.containsKey("externalParsers")) {
        List<Parameters> externalParsers = parameters.getAsList("externalParsers", Parameters.class);
        for (Parameters extP : externalParsers) {
          fileTypeMap.put(extP.getString("filetype"),
                  Class.forName(extP.getString("class")));
        }
      }
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalArgumentException(cnfe);
    }
  }

  @Override
  public void process(DocumentSplit split) throws IOException {

    LOG.info("Processing split: "+split.fileName);

    DocumentStreamParser parser = null;
    long count = 0;
    long limit = Long.MAX_VALUE;
    if (split.startKey.length > 0) {
      if (Utility.compare(subCollCheck, split.startKey) == 0) {
        limit = Utility.uncompressLong(split.endKey, 0);
      }
    }

    // Determine the file type either from the parameters
    // or from the guess in the splits
    String fileType;
    if (parameters.containsKey("filetype")) {
      fileType = parameters.getString("filetype");
    } else {
      fileType = split.fileType;
    }

    if (fileTypeMap.containsKey(fileType)) {
      try {
        parser = constructParserWithSplit(fileTypeMap.get(fileType), split, parameters);
        assert(parser != null);
      } catch (EOFException ee) {
        System.err.printf("Found empty split %s. Skipping due to no content.", split.toString());
        return;
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        if(parser != null) parser.close();
      }
    } else {
      throw new IOException("Unknown fileType: " + fileType
              + " for fileName: " + split.fileName);
    }

    // A parser is instantiated. Start producing documents for consumption
    // downstream.
    try {
      Document document;
      long fileDocumentCount = 0;
      while ((document = parser.nextDocument()) != null) {
        document.filePath = split.fileName;
        document.fileLocation = fileDocumentCount;
        fileDocumentCount++;

        document.fileId = split.fileId;
        document.totalFileCount = split.totalFileCount;
        processor.process(document);
        if (documentCounter != null) {
          documentCounter.increment();
        }
        count++;

        // Enforces limitations imposed by the endKey subcollection specifier.
        // See DocumentSource for details.
        if (count >= limit) {
          break;
        }

        if (count % 10000 == 0) {
          LOG.log(Level.WARNING, "Read " + count + " from split: " + split.fileName);
        }
      }

      LOG.info("Processed " + count + " total in split: " + split.fileName);
    } finally {
      parser.close();
    }
  }

  private static DocumentStreamParser constructParserWithSplit(Class parserClass, DocumentSplit split, Parameters parameters) throws IOException {
    for(Constructor cons : parserClass.getConstructors()) {
      Class<?>[] formals = cons.getParameterTypes();
      if(formals.length != 2) continue;
      if(formals[0].isAssignableFrom(DocumentSplit.class) && formals[1].isAssignableFrom(Parameters.class)) {
        try {
          return (DocumentStreamParser) cons.newInstance(split, parameters);
        } catch (InstantiationException e) {
          throw new IOException(e);
        } catch (IllegalAccessException e) {
          throw new IOException(e);
        } catch (InvocationTargetException e) {
          throw new IOException(e);
        }
      }
    }
    // None of the constructors worked. Complain.
    throw new IllegalArgumentException("No viable constructor for file type parser " + parserClass.getName() +  "\n\n" +
        "Expected (DocumentSplit split, Parameters p) in the constructor.\n");
  }

  /** returns true if internally defined */
  public static boolean isParsable(String extension) {
    for (String[] entry : sFileTypeLookup) {
      if (entry[0].equals(extension)) {
        return true;
      }
    }
    return false;
  }
}
