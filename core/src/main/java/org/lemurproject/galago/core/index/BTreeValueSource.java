package org.lemurproject.galago.core.index;

import java.io.IOException;

/**
 *
 * @author jfoley
 */
public abstract class BTreeValueSource implements DataSource {
  // OPTIONS
  public static final int HAS_SKIPS = 0x01;
  public static final int HAS_MAXTF = 0x02;
  public static final int HAS_INLINING = 0x04;
  
  protected BTreeReader.BTreeIterator btreeIter;
  protected byte[] key;
  
  public BTreeValueSource(BTreeReader.BTreeIterator it) throws IOException {
    this.key = it.getKey();
    btreeIter = it;
    reset();
  }
  
  @Override
  public boolean hasMatch(int id) {
    return !isDone() && currentCandidate() == id;
  }
}
