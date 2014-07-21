// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @author jfoley
 */
public class CmpUtil {
  public static final double epsilon = 0.5 * Math.pow(10, -10);
  public static final double neg_epsilon = -1.0 * epsilon;

  public static int compare(int one, int two) {
    return one - two;
  }

  public static int compare(long one, long two) {
    return (int) (one - two);
  }

  public static int compare(double one, double two) {
    return Double.compare(one, two);
  }

  public static int compare(float one, float two) {
    return Float.compare(one, two);
  }

  public static int compare(String one, String two) {
    return one.compareTo(two);
  }

  public static int compare(byte[] one, byte[] two) {
    int sharedLength = Math.min(one.length, two.length);

    for (int i = 0; i < sharedLength; i++) {
      int a = ((int) one[i]) & 0xFF;
      int b = ((int) two[i]) & 0xFF;
      int result = a - b;

      if (result < 0) {
        return -1;
      }
      if (result > 0) {
        return 1;
      }
    }

    return one.length - two.length;
  }

  public static boolean equals(byte[] one, byte[] two) {
    if(one.length != two.length) return false;

    for (int i = 0; i < one.length; i++) {
      if(one[i] != two[i]) return false;
    }

    return true;
  }

  // comparator for byte arrays
  public static class ByteArrComparator implements Comparator<byte[]>, Serializable {
    @Override
    public int compare(byte[] a, byte[] b) {
      return CmpUtil.compare(a, b);
    }
  }

  public static int hash(byte b) {
    return ((int) b) & 0xFF;
  }

  public static int hash(int i) {
    return i;
  }

  public static int hash(long l) {
    return (int) l;
  }

  public static int hash(double d) {
    return (int) (d * 100000);
  }

  public static int hash(float f) {
    return (int) (f * 100000);
  }

  public static int hash(String s) {
    return s.hashCode();
  }

  public static int hash(byte[] bytes) {
    int h = 0;
    for (byte b : bytes) {
      h += 7 * h + b;
    }
    return h;
  }
}
