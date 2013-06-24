package org.lemurproject.galago.core.index.source;

import java.io.IOException;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class MemValueSource<T> implements DiskSource {

  // OPTIONS
  public static final int HAS_SKIPS = 0x01;
  public static final int HAS_MAXTF = 0x02;
  public static final int HAS_INLINING = 0x04;
  final protected byte[] key;

  public MemValueSource(byte[] key) throws IOException {
    this.key = key;
  }

  @Override
  public boolean hasMatch(long id) {
    return !isDone() && currentCandidate() == id;
  }

  @Override
  public String key() {
    return Utility.toString(key);
  }
}
