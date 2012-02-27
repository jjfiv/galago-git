// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Iterates over the a KeyIterator as if it were a Value iterator.
 * Useful for iterating over document lengths or document names.
 *
 *
 * @author marc
 */
public abstract class KeyToListIterator extends MovableValueIterator {

  protected KeyIterator iterator;

  public KeyToListIterator(KeyIterator ki) {
    iterator = ki;
  }

  @Override
  public boolean next() throws IOException {
    return iterator.nextKey();
  }

  @Override
  public boolean moveTo(int identifier) throws IOException {
    return iterator.skipToKey(Utility.fromInt(identifier));
  }

  @Override
  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
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
      return Utility.toInt(iterator.getKey());
    } catch (IOException ioe) {
      return Integer.MAX_VALUE;
    }
  }

  @Override
  public boolean atCandidate(int identifier) {
    return (currentCandidate() == identifier);
  }
  
  @Override
  public int compareTo(MovableIterator other) {
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
