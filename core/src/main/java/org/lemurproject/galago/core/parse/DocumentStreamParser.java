// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.*;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.zip.ZipFile;

/**
 * This is the interface to Galago's abstract parser system.
 * All parsers inherit from this abstract class and are created here via reflection.
 *
 * @see org.lemurproject.galago.core.parse.DocumentSource
 * @see org.lemurproject.galago.core.util.DocumentSplitFactory
 * @author trevor, sjh
 */
public abstract class DocumentStreamParser implements Closeable {
  private static Logger log = Logger.getLogger(DocumentStreamParser.class.getName());

  /* Static interface */

  // The built-in type map
  public static Map<String, Class> fileTypeMap;
  static {
    fileTypeMap = new ConcurrentHashMap<>();
    addExternalParsers(Parameters.create());
  }

  // isParsable
  public static boolean hasParserForExtension(String ext) {
    return fileTypeMap.containsKey(ext);
  }

  // configuring extra parsers
  public static void addExternalParsers(Parameters parameters) {
    try {
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

  /** @deprecated use create instead */
  @Deprecated
  public static DocumentStreamParser instance(DocumentSplit split, Parameters parameters) throws IOException {
    return create(split, parameters);
  }
  // create a parser
  public static DocumentStreamParser create(DocumentSplit split, Parameters parameters) throws IOException {
    // Determine the file type either from the parameters
    // or from the guess in the splits
    String fileType;
    if (parameters.containsKey("filetype")) {
      fileType = parameters.getString("filetype");
    } else {
      fileType = split.fileType;
    }

    // see if filetype is in our list of known types
    if (fileTypeMap.containsKey(fileType)) {
       return constructParserWithSplit(fileTypeMap.get(fileType), split, parameters);
    }

    // see if filetype is a full class, and can be instantiated that way.
    try {
      Class<?> clazz = Class.forName(fileType);
      if(clazz != null) {
        // cache it
        fileTypeMap.put(fileType, clazz);
        return constructParserWithSplit(clazz, split, parameters);
      }
    } catch (ClassNotFoundException e) {
      // not actually a valid class
    }

    throw new IOException("Unknown fileType: " + fileType + " for fileName: " + split.fileName);
  }

  private static DocumentStreamParser constructParserWithSplit(Class parserClass, DocumentSplit split, Parameters parameters) throws IOException {
    for(Constructor cons : parserClass.getConstructors()) {
      Class<?>[] formals = cons.getParameterTypes();
      if(formals.length != 2) continue;
      if(formals[0].isAssignableFrom(DocumentSplit.class) && formals[1].isAssignableFrom(Parameters.class)) {
        try {
          return (DocumentStreamParser) cons.newInstance(split, parameters);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
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

  @Override
  public abstract void close() throws IOException;

  /*** static functions for opening files ***/

  public static BufferedReader getBufferedReader(DocumentSplit split) throws IOException {
    if(split.innerName.isEmpty())
      return new BufferedReader(new InputStreamReader(StreamCreator.openInputStream(split.fileName), ByteUtil.utf8));

    // else zip file:
    ZipFile zipFile = ZipUtil.open(split.fileName);
    return new BufferedReader(new InputStreamReader(ZipUtil.streamZipEntry(zipFile, split.innerName), ByteUtil.utf8));
  }

  public static BufferedInputStream getBufferedInputStream(DocumentSplit split) throws IOException {
    if(split.innerName.isEmpty())
      return new BufferedInputStream(StreamCreator.openInputStream(split.fileName));

    // else zip file:
    ZipFile zipFile = ZipUtil.open(split.fileName);
    return new BufferedInputStream(ZipUtil.streamZipEntry(zipFile, split.innerName));
  }

  public static String getFullPath(DocumentSplit split) {
    if(split.innerName.isEmpty())
      return split.fileName;
    return split.fileName+"!"+split.innerName;
  }

  public static String getFileName(DocumentSplit split) {
    if(split.innerName.isEmpty())
      return split.fileName;
    return split.innerName;
  }

  public static String getExtension(DocumentSplit split) {
    return FSUtil.getExtension(new File(getFileName(split)));
  }
}
