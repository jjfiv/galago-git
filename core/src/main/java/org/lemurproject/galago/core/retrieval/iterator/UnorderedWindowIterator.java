// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author trevor
 */
public class UnorderedWindowIterator extends ExtentConjunctionIterator {

  int width;
  boolean overlap;

  /**
   * Creates a new instance of UnorderedWindowIterator
   */
  public UnorderedWindowIterator(NodeParameters parameters, MovableExtentIterator[] evIterators) throws IOException {
    super(parameters, evIterators);
    this.width = (int) parameters.get("default", -1);
    this.overlap = parameters.get("overlap", false);
    syncTo(0);
  }

  public void loadExtents() {
    int document = currentCandidate();
    if (isDone() || this.extents.getDocument() == document) {
      return;
    }
    extents.reset();
    extents.setDocument(document);

    ExtentArrayIterator[] arrayIterators;
    int maximumPosition = 0;
    int minimumPosition = Integer.MAX_VALUE;

    // someday this will be a heap/priorityQueue for the overlapping case
    arrayIterators = new ExtentArrayIterator[iterators.length];

    for (int i = 0; i < iterators.length; i++) {
      if (iterators[i].isDone()
              || !iterators[i].hasMatch(document)) {
        // we can not load any extents if the iterator is done - or is at the wrong document.
        return;
      }

      arrayIterators[i] = new ExtentArrayIterator(((MovableExtentIterator) iterators[i]).extents());

      if (arrayIterators[i].isDone()) {
        // if this document does not have any extents we can not load any extents
        return;
      }

      minimumPosition = Math.min(arrayIterators[i].currentBegin(), minimumPosition);
      maximumPosition = Math.max(arrayIterators[i].currentEnd(), maximumPosition);
    }


    do {
      boolean match = (maximumPosition - minimumPosition <= width) || (width == -1);
      // try to emit an extent here, but only if the width is small enough
      if (match) {
        extents.add(minimumPosition, maximumPosition);
      }

      for (int i = 0; i < arrayIterators.length; i++) {
        if (arrayIterators[i].currentBegin() == minimumPosition) {
          boolean result = arrayIterators[i].next();
          if (!result) {
            return;
          }
        }
      }

      // reset the minimumPosition
      minimumPosition = Integer.MAX_VALUE;
      maximumPosition = 0;

      // now, reset bounds
      for (int i = 0; i < arrayIterators.length; i++) {
        minimumPosition = Math.min(minimumPosition, arrayIterators[i].currentBegin());
        maximumPosition = Math.max(maximumPosition, arrayIterators[i].currentEnd());
      }
    } while (true);
  }
}
