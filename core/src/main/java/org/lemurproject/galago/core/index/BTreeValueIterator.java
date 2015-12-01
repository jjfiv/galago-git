// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.btree.BTreeIterator;

import java.io.IOException;

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

  public abstract void reset(BTreeIterator it) throws IOException;

  public String getKeyString() throws IOException {
    return ByteUtil.toString(key);
  }
}
