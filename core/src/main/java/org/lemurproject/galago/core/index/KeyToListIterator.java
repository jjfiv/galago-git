// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Iterates over the a KeyIterator as if it were a Value iterator.
 * Useful for iterating over document lengths or document names.
 *
 *
 * @author marc
 */
public abstract class KeyToListIterator implements ValueIterator {

  protected KeyIterator iterator;

  public KeyToListIterator(KeyIterator ki) {
    iterator = ki;
  }

  public boolean next() throws IOException {
    return iterator.nextKey();
  }

  public boolean moveTo(int identifier) throws IOException {
    return iterator.skipToKey(Utility.fromInt(identifier));
  }

  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  public void reset() throws IOException {
    iterator.reset();
  }

  public boolean isDone() {
    return iterator.isDone();
  }

  public int currentCandidate() {
    try {
      return Utility.toInt(iterator.getKeyBytes());
    } catch (IOException ioe) {
      return Integer.MAX_VALUE;
    }
  }

  public boolean hasMatch(int identifier) {
    return (currentCandidate() == identifier);
  }

  public int compareTo(ValueIterator other) {
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
