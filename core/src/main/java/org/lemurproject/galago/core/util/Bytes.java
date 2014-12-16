// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.util;

import java.util.Arrays;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.CmpUtil;

/**
 * Boxing a byte[] object to ensure proper operation
 *
 * @author irmarc
 */
public class Bytes implements Comparable<Bytes> {
  byte[] bytes;

  public Bytes(byte[] b) {
    bytes = Arrays.copyOf(b, b.length);
  }

  public boolean equals(Bytes that) {
    return Arrays.equals(this.bytes, that.bytes);
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.bytes);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Bytes other = (Bytes) obj;
    if (!Arrays.equals(this.bytes, other.bytes)) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(Bytes that) {
    return CmpUtil.compare(this.bytes, that.bytes);
  }
}
