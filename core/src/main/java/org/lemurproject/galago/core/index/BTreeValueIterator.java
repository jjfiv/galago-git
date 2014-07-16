// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.ByteUtil;

/**
 * This is the base type of most Iterators (previously KeyListReader.ListIterator)
 * @author jfoley
 */
public abstract class BTreeValueIterator implements BaseIterator {
  // OPTIONS
  public static final int HAS_SKIPS = 0x01;
  public static final int HAS_MAXTF = 0x02;
  public static final int HAS_INLINING = 0x04;
  protected byte[] key;

  public BTreeValueIterator(byte[] key) {
    this.key = key;
  }

  public abstract void reset(BTreeReader.BTreeIterator it) throws IOException;

  public String getKeyString() throws IOException {
    return ByteUtil.toString(key);
  }

  @Override
  public int compareTo(BaseIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return Utility.compare(currentCandidate(), other.currentCandidate());
  }
  
}
