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
   * Copies the data from the string s to the file.
   * @throws java.io.IOException
   */
  public static void copyStringToFile(String s, File file) throws IOException {
    DataOutputStream output = null;
    try {
      FSUtil.makeParentDirectories(file);
      output = StreamCreator.openOutputStream(file);
      output.write(ByteUtil.fromString(s));
    } finally {
      if(output != null) output.close();
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

  public static String getResourceAsString(String path) throws IOException {
    return copyStreamToString(StreamUtil.class.getResourceAsStream(path));
  }

  /**
   * Copies the data from the InputStream to a file, then closes both when
   * finished.
   *
   * @throws java.io.IOException
   */
  public static void copyStreamToFile(InputStream stream, File file) throws IOException {
    DataOutputStream output = null;
    try {
      output = StreamCreator.openOutputStream(file);
      final int oneMegabyte = 1 * 1024 * 1024;
      byte[] data = new byte[oneMegabyte];

      while (true) {
        int bytesRead = stream.read(data);

        if (bytesRead < 0) {
          break;
        }
        output.write(data, 0, bytesRead);
      }
    } finally {
      stream.close();
      if(output != null) output.close();
    }
  }
}
