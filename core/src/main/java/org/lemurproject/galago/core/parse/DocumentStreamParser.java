// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.StreamCreator;
import org.tukaani.xz.XZInputStream;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author trevor, sjh
 */
public abstract class DocumentStreamParser {

  /* Static interface */

  // The built-in type map
  public static HashMap<String, Class> fileTypeMap;
  static {
    fileTypeMap = new HashMap<String, Class>();
    fileTypeMap.put("html", FileParser.class);
    fileTypeMap.put("xml", FileParser.class);
    fileTypeMap.put("txt", FileParser.class);
    fileTypeMap.put("arc", ArcParser.class);
    fileTypeMap.put("warc", WARCParser.class);
    fileTypeMap.put("trectext", TrecTextParser.class);
    fileTypeMap.put("trecweb", TrecWebParser.class);
    fileTypeMap.put("twitter", TwitterParser.class);
    fileTypeMap.put("corpus", CorpusSplitParser.class);
    fileTypeMap.put("selectivecorpus", CorpusSelectiveSplitParser.class);
    fileTypeMap.put("wiki", WikiParser.class);
  }

  // isParsable
  public static boolean hasParserForExtension(String ext) {
    return fileTypeMap.containsKey(ext);
  }

  // configuring extra parsers
  public static void addExternalParsers(Parameters parameters) {
    try {
      // Look for external mapping definitions
      if (parameters.containsKey("externalParsers")) {
        for (Parameters extP : parameters.getAsList("externalParsers", Parameters.class)) {
          fileTypeMap.put(extP.getString("filetype"), Class.forName(extP.getString("class")));
        }
      }
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalArgumentException(cnfe);
    }
  }

  // create a parser
  public static DocumentStreamParser instance(DocumentSplit split, Parameters parameters) throws IOException {
    // Determine the file type either from the parameters
    // or from the guess in the splits
    String fileType;
    if (parameters.containsKey("filetype")) {
      fileType = parameters.getString("filetype");
    } else {
      fileType = split.fileType;
    }

    if (fileTypeMap.containsKey(fileType)) {
      return constructParserWithSplit(fileTypeMap.get(fileType), split, parameters);
    } else {
      throw new IOException("Unknown fileType: " + fileType + " for fileName: " + split.fileName);
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


  /** This is the constructor expected by UniversalParser
   *  It must be implemented in each implementing class
   */
  public DocumentStreamParser(DocumentSplit split, Parameters p) { }

  public abstract Document nextDocument() throws IOException;

  public abstract void close() throws IOException;

  /*** static functions for opening files ***/

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

  public static BufferedInputStream getBufferedInputStream(DocumentSplit split) throws IOException {
    FileInputStream fileStream = StreamCreator.realInputStream(split.fileName);
    BufferedInputStream stream;

    if (split.isCompressed) {
      // Determine compression algorithm
      if (split.fileName.endsWith("gz")) { // Gzip
        stream = new BufferedInputStream(new GZIPInputStream(fileStream));
      } else if (split.fileName.endsWith("xz")) { // xz
        stream = new BufferedInputStream(new XZInputStream(fileStream));
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
