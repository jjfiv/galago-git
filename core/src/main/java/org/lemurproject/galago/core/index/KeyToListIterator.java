// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Iterates over the a KeyIterator as if it were a Value iterator. Useful for
 * iterating over document lengths or document names.
 *
 * @author irmarc
 */
public abstract class KeyToListIterator extends DiskIterator {

  protected KeyIterator iterator;

  public KeyToListIterator(KeyIterator ki) {
    iterator = ki;
  }

  @Override
  public void syncTo(long identifier) throws IOException {
    iterator.skipToKey(Utility.fromLong(identifier));
  }

  @Override
  public void movePast(long identifier) throws IOException {
    iterator.skipToKey(Utility.fromLong(identifier + 1));
  }

  @Override
  public void reset() throws IOException {
    iterator.reset();
  }

  @Override
  public boolean isDone() {
    return iterator.isDone();
  }

  @Override
  public int currentCandidate() {
    try {
      return (int) Utility.toLong(iterator.getKey());
    } catch (IOException ioe) {
      return Integer.MAX_VALUE;
    }
  }

  @Override
  public boolean hasMatch(long identifier) {
    return (!isDone() && currentCandidate() == identifier);
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
    return currentCandidate() - other.currentCandidate();
  }
}
