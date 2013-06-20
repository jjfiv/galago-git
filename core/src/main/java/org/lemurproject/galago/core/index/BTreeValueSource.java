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
  
  public BTreeValueSource(BTreeReader.BTreeIterator it) throws IOException {
    reset(it);
  }
  
  final public void reset(BTreeReader.BTreeIterator it) throws IOException {
    btreeIter = it;
    reset();
  }
  
  @Override
  public boolean hasMatch(int id) {
    return !isDone() && currentCandidate() == id;
  }
}
