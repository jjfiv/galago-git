// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility;

import java.nio.charset.Charset;

/**
 * @author jfoley
 */
public class ByteUtil {
  private static Charset utf8 = Charset.forName("UTF-8");

  public static String toString(byte[] word) {
    return new String(word, utf8);
  }

  public static byte[] fromString(String word) {
    return word.getBytes(utf8);
  }
}
