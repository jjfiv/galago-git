// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility;

import java.io.*;

/**
 * @author jfoley
 */
public class StreamUtil {

  /**
   * Copies data from the input stream to the output stream.
   *
   * @param input The input stream.
   * @param output The output stream.
   * @throws java.io.IOException
   */
  public static void copyStream(InputStream input, OutputStream output) throws IOException {
    byte[] data = new byte[65536];
    while (true) {
      int bytesRead = input.read(data);
      if (bytesRead < 0) {
        break;
      }
      output.write(data, 0, bytesRead);
    }
  }

  /**
   * Copies data from the input stream and returns a String (UTF-8 if not specified)
   */
  public static String copyStreamToString(InputStream inputStream, String encoding) throws IOException {
    encoding = (encoding == null) ? "UTF-8" : encoding;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    copyStream(inputStream, baos);
    return baos.toString(encoding);
  }

  public static String copyStreamToString(InputStream input) throws IOException {
    return copyStreamToString(input, "UTF-8");
  }

  /**
   * Copies the data from file into the stream. Note that this method does not
   * close the stream (in case you want to put more in it).
   *
   * @throws java.io.IOException
   */
  public static void copyFileToStream(File file, OutputStream stream) throws IOException {
    InputStream input = null;
    try {
      input = new FileInputStream(file);
      copyStream(input, stream);
    } finally {
      if(input != null) input.close();
    }
  }

}
