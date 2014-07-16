// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility;

import java.nio.charset.Charset;

/**
 * @author jfoley
 */
public class ByteUtil {
  public static Charset utf8 = Charset.forName("UTF-8");
  public static final byte[] EmptyArr = new byte[0];

  public static String toString(byte[] word) {
    return new String(word, utf8);
  }

  public static byte[] fromString(String word) {
    return word.getBytes(utf8);
  }

  public static String toString(byte[] buffer, int offset, int len) {
    return new String(buffer, offset, len, utf8);
  }

  public static String toString(byte[] buffer, int len) {
    return new String(buffer, 0, len, utf8);
  }
}