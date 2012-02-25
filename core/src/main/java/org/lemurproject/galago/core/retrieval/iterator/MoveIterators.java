// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;

/**
 *
 * @author trevor
 */
public class MoveIterators {

  /**
   * Moves all iterators in the array to the same intID.  This method will only
   * stop at documents where all of the iterators have a match.
   *
   * @return The currentIdentifier number that the iterators are now pointing to,
   *         or Integer.MAX_VALUE; if one of the iterators is now done.
   */
  public static int moveAllToSameDocument(ValueIterator[] iterators) throws IOException {

    int currentTarget = findMaximumDocument(iterators);

    if (currentTarget == Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }

    boolean allMatch = false;

    while (!allMatch) {
      allMatch = true;

      for (ValueIterator iterator : iterators) {

        if (!iterator.moveTo(currentTarget)) {
          allMatch = false;
          // we could have failed to move to taget because the iterator isDone.
          if (iterator.isDone()) {
            return Integer.MAX_VALUE;

          } else {
            // this iterator points after the target currentIdentifier,
            // so the target currentIdentifier is not a match.
            // we break and try again because we don't want to
            // touch the longest iterators if we can help it.
            if (currentTarget == iterator.currentCandidate()) {
              // if the next target is identical to the current -> then add one.
              currentTarget++;
            } else {
              currentTarget = iterator.currentCandidate();
            }
            break;
          }
        }
      }
    }

    return currentTarget;
  }

  public static boolean allHasMatch(ValueIterator[] iterators, int document) {
    if (iterators.length == 0) {
      return true;
    }

    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
        if (!iterator.atCandidate(document)) {
          return false;
        }
      }
    }

    return true;
  }
  
  public static boolean anyHasMatch(ValueIterator[] iterators, int document) {
    if (iterators.length == 0) {
      return true;
    }

    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
        if (iterator.atCandidate(document)) {
          return true;
        }
      }
    }

    return false;
  }
  

  public static boolean allSameDocument(ValueIterator[] iterators) {
    if (iterators.length == 0) {
      return true;
    }
    int document = iterators[0].currentCandidate();

    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
        if (document != iterator.currentCandidate()) {
          return false;
        }
      }
    }

    return true;
  }

  public static int findMaximumDocument(ValueIterator[] iterators) {
    int maximumDocument = 0;

    for (ValueIterator iterator : iterators) {
      if (iterator.isDone()) {
        return Integer.MAX_VALUE;
      }
      maximumDocument = Math.max(maximumDocument, iterator.currentCandidate());
    }

    return maximumDocument;
  }

  public static int findMinimumDocument(ValueIterator[] iterators) {
    int minimumDocument = Integer.MAX_VALUE;

    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
        minimumDocument = Math.min(minimumDocument, iterator.currentCandidate());
      }
    }

    return minimumDocument;
  }
}
