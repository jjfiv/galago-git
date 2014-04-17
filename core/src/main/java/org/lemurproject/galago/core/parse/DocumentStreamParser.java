// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.StreamCreator;
import org.tukaani.xz.XZInputStream;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author trevor, sjh
 */
public abstract class DocumentStreamParser {

  /** This is the constructor expected by UniversalParser
   *  It must be implemented in each implementing class
   */
  public DocumentStreamParser(DocumentSplit split, Parameters p) {}

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
