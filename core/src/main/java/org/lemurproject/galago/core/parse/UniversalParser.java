// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.StreamCreator;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * Determines the class type of the input split, either based
 * on the "filetype" parameter passed in, or by guessing based on
 * the file path extension.
 *
 * (7/29/2012, irmarc): Refactored to be plug-and-play. External filetypes
 * may be added via the parameters. 
 *
 * Instantiation of a type-specific parser (TSP) is done by the UniversalParser. 
 * It checks the formal argument types of the (TSP) to match on the possible
 * input methods it has available (i.e. an inputstream or a buffered reader over the
 * input data. Additionally, any TSP may have TupleFlowParameters in its formal argument
 * list, and the parameters provided to the UniversalParser will be forwarded to the
 * TSP instance.
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
	{"wiki", WikiParser.class.getName()},
	{"mbtei.page", MBTEIPageParser.class.getName()},
	{"mbtei.book", MBTEIBookParser.class.getName()},
	{"mbtei.entity", MBTEIEntityParser.class.getName()},
	{"mbtei.person", MBTEIPersonParser.class.getName()},
	{"mbtei.location", MBTEILocationParser.class.getName()}
    };

  private HashMap<String, Class> fileTypeMap;
  private Counter documentCounter;
  private TupleFlowParameters tfParameters;
  private Parameters parameters;
  private Logger logger = Logger.getLogger(getClass().toString());
  private byte[] subCollCheck = "subcoll".getBytes();

  public UniversalParser(TupleFlowParameters parameters) {
    this.tfParameters = parameters;
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
	      List<Parameters> externalParsers =
		  (List<Parameters>) parameters.getAsList("externalParsers");
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
	    parser = constructParserWithSplit(fileTypeMap.get(fileType), split);
	} catch (EOFException ee) {
	    System.err.printf("Found empty split %s. Skipping due to no content.", split.toString());
	    return;
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}
    } else {
        throw new IOException("Unknown fileType: " + fileType
			      + " for fileName: " + split.fileName);
    }

    // A parser is instantiated. Start producing documents for consumption 
    // downstream.
    Document document;
    while ((document = parser.nextDocument()) != null) {
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
    }
    
    if (parser != null) {
	parser.close();
    }
  }

  // Try like Hell to match up the formal parameter list with the available
  // objects/methods in this class.
  // 
  // Longest constructor is built first.
  private DocumentStreamParser constructParserWithSplit(Class parserClass, 
							DocumentSplit split) 
      throws IOException, InstantiationException, IllegalAccessException,
      InvocationTargetException {
      Constructor[] constructors = parserClass.getConstructors();
      Arrays.sort(constructors, new Comparator<Constructor>() {
	      public int compare(Constructor c1, Constructor c2) {
		  return (c2.getParameterTypes().length -
			  c1.getParameterTypes().length);
	      }
	  });
      Class[] formals;
      ArrayList<Object> actuals;
      for (Constructor constructor : constructors) {
	  formals = constructor.getParameterTypes();
	  actuals = new ArrayList<Object>(formals.length);
	  for (Class formalClass : formals) {
	      if (formalClass.isAssignableFrom(BufferedInputStream.class)) {
		  actuals.add(getLocalBufferedInputStream(split));
	      } else if (formalClass.isAssignableFrom(BufferedReader.class)) {
		  actuals.add(getLocalBufferedReader(split));
	      } else if (String.class.isAssignableFrom(formalClass)) {
		  actuals.add(split.fileName);
	      } else if (DocumentSplit.class.isAssignableFrom(formalClass)) {
		  actuals.add(split);
	      } else if (Parameters.class.isAssignableFrom(formalClass)) {
		  actuals.add(parameters);
	      } else if (TupleFlowParameters.class.isAssignableFrom(formalClass)) {
		  actuals.add(tfParameters);
	      }
	  }
	  if (actuals.size() == formals.length) {
	      return (DocumentStreamParser) 
		  constructor.newInstance(actuals.toArray(new Object[0]));
	  }
      }
      // None of the constructors worked. Complain.
      StringBuilder builder = new StringBuilder();
      builder.append("No viable constructor for file type parser ");
      builder.append(parserClass.getName()).append("\n\n");      
      builder.append("Valid formal parameters include TupleFlowParameters,");
      builder.append(" Parameters, BufferedInputStream or BufferedReader,\n");
      builder.append(" String (fileName is passed as the actual), or");
      builder.append(" DocumentSplit.\n\nConstuctors found:\n");
      for (Constructor c : constructors) {
	  builder.append("( ");
	  formals = c.getParameterTypes();
	  for (Class klazz : formals) {
	      builder.append(klazz.getName()).append(",");
	  }
	  builder.append(")\n");
      }
      throw new IllegalArgumentException(builder.toString());
  }

  public static boolean isParsable(String extension) {
      for (String[] entry : sFileTypeLookup) {
	  if (entry[0].equals(extension)) {
	      return true;
	  }
      }
      return false;
  }

  public BufferedReader getLocalBufferedReader(DocumentSplit split) throws IOException {
    BufferedReader br = getBufferedReader(split);
    return br;
  }

  public static BufferedReader getBufferedReader(DocumentSplit split) throws IOException {
    FileInputStream stream = StreamCreator.realInputStream(split.fileName);
    BufferedReader reader;

    if (split.isCompressed) {
      // Determine compression type
      if (split.fileName.endsWith("gz")) { // Gzip
        reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(stream)));
      } else { // BZip2
        BufferedInputStream bis = new BufferedInputStream(stream);
        //bzipHeaderCheck(bis);
        reader = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(bis)));
      }
    } else {
      reader = new BufferedReader(new InputStreamReader(stream));
    }
    return reader;
  }

  public BufferedInputStream getLocalBufferedInputStream(DocumentSplit split) throws IOException {
    BufferedInputStream bis = getBufferedInputStream(split);
    return bis;
  }

  public static BufferedInputStream getBufferedInputStream(DocumentSplit split) throws IOException {
    FileInputStream fileStream = StreamCreator.realInputStream(split.fileName);
    BufferedInputStream stream;

    if (split.isCompressed) {
      // Determine compression algorithm
      if (split.fileName.endsWith("gz")) { // Gzip
        stream = new BufferedInputStream(new GZIPInputStream(fileStream));
      } else { // bzip2
        BufferedInputStream bis = new BufferedInputStream(fileStream);
        stream = new BufferedInputStream(new BZip2CompressorInputStream(bis));
      }
    } else {
      stream = new BufferedInputStream(fileStream);
    }
    return stream;
  }
}
