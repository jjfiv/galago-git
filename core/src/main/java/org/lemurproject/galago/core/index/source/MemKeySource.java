// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.source;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * This class treats a TreeMap or at least the keys of a BTree as a data
 * source. (each key is a document identifier)
 *
 * @author sjh
 * @see BTreeMemSource
 */
public abstract class MemKeySource<T> implements DiskSource {

  protected TreeMap<byte[], T> reader;
  protected NavigableMap<byte[], T> dataIterator;
  protected byte[] currKey;
  protected final long keyCount;

  public MemKeySource(TreeMap data) throws IOException {
    reader = data;
    keyCount = reader.size();
    reset();
  }

  /**
   * This fetches a new BTreeIterator from the BTreeReader.
   *
   * Implementing classes will need to call this reset explicitly if they want
   * to do anything remotely interesting in their own reset methods.
   *
   * @throws IOException
   */
  @Override
  public void reset() throws IOException {
    dataIterator = reader.tailMap(reader.firstKey(), true);
    loadNextKey();
  }

  @Override
  public boolean isDone() {
    return currKey == null;
  }

  @Override
  public long currentCandidate() {
    return Utility.toLong(currKey);
  }

  @Override
  public void syncTo(long id) throws IOException {
    dataIterator = dataIterator.tailMap(Utility.fromLong(id), true);
    loadNextKey();
  }

  @Override
  public void movePast(long id) throws IOException {
    syncTo(id + 1);
  }

  @Override
  public long totalEntries() {
    return keyCount;
  }

  private void loadNextKey() {
    if (!dataIterator.isEmpty()) {
      currKey = dataIterator.firstKey();
    } else {
      currKey = null;
    }
  }
}
