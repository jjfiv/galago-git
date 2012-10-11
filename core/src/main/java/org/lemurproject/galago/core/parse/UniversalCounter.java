// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.List;
import java.util.logging.Logger;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Determines the class type of the input split, either based on the "filetype"
 * parameter passed in, or by guessing based on the file path extension.
 *
 * (7/29/2012, irmarc): Refactored to be plug-and-play. External filetypes may
 * be added via the parameters.
 *
 * Instantiation of a type-specific parser (TSP) is done by the UniversalParser.
 * It checks the formal argument types of the (TSP) to match on the possible
 * input methods it has available (i.e. an inputstream or a buffered reader over
 * the input data. Additionally, any TSP may have TupleFlowParameters in its
 * formal argument list, and the parameters provided to the UniversalParser will
 * be forwarded to the TSP instance.
 *
 * @author trevor, sjh, irmarc
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class UniversalCounter extends StandardStep<DocumentSplit, KeyValuePair> {

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
    {"wiki", WikiParser.class.getName()},
    {"mbtei.page", MBTEIPageParser.class.getName()},
    {"mbtei.book", MBTEIBookParser.class.getName()},
    {"mbtei.entity", MBTEIEntityParser.class.getName()},
    {"mbtei.person", MBTEIPersonParser.class.getName()},
    {"mbtei.location", MBTEILocationParser.class.getName()}
  };
  private HashMap<String, Class> documentStreamParser;
  private Counter documentCounter;
  private TupleFlowParameters tfParameters;
  private Parameters parameters;
  private Logger logger = Logger.getLogger(getClass().toString());
  private byte[] subCollCheck = "subcoll".getBytes();

  public UniversalCounter(TupleFlowParameters parameters) {
    this.documentCounter = parameters.getCounter("Documents Parsed");
    this.tfParameters = parameters;
    this.parameters = parameters.getJSON();
    buildFileTypeMap();
  }

  private void buildFileTypeMap() {
    try {
      documentStreamParser = new HashMap<String, Class>();
      for (String[] mapping : sFileTypeLookup) {
        documentStreamParser.put(mapping[0], Class.forName(mapping[1]));
      }

      // Look for external mapping definitions
      if (parameters.containsKey("externalParsers")) {
        List<Parameters> externalParsers =
                (List<Parameters>) parameters.getAsList("externalParsers");
        for (Parameters extP : externalParsers) {
          documentStreamParser.put(extP.getString("filetype"),
                  Class.forName(extP.getString("class")));
        }
      }
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalArgumentException(cnfe);
    }
  }

  public boolean isParsable(String extension) {
    return this.documentStreamParser.containsKey(extension);
  }

  @Override
  public void process(DocumentSplit split) throws IOException {
    long limit = Long.MAX_VALUE;
    if (split.startKey.length > 0) {
      if (Utility.compare(subCollCheck, split.startKey) == 0) {
        limit = Utility.uncompressLong(split.endKey, 0);
      }
    }

    if (this.documentStreamParser.containsKey(split.fileType)) {
      try {
        Class c = documentStreamParser.get(split.fileType);
        Constructor cstr = c.getConstructor(DocumentSplit.class, Parameters.class);
        DocumentStreamParser parser = (DocumentStreamParser) cstr.newInstance(split, parameters);

        Document document;
        long count = 0;
        while ((document = parser.nextDocument()) != null) {
          document.fileId = split.fileId;
          document.totalFileCount = split.totalFileCount;

          if (documentCounter != null) {
            documentCounter.increment();
          }

          count++;

          // Enforces limitations imposed by the endKey subcollection specifier.
          // See DocumentSource for details.
          if (count >= limit) {
            break;
          }
        }

        if (parser != null) {
          parser.close();
        }

        KeyValuePair kvp = new KeyValuePair(Utility.fromString(split.fileName), Utility.fromLong(count));
        processor.process(kvp);

      } catch (Exception ex) {
        logger.log(Level.INFO, "Failed to parse document split - {0} as {1}\n", new Object[]{split.toString(), split.fileType});
        logger.log(Level.SEVERE, ex.toString());
      }
    } else {
      logger.log(Level.INFO, "Ignoring {0} - could not find a parser for file-type:{1}\n", new Object[]{split.toString(), split.fileType});
    }
  }
}
