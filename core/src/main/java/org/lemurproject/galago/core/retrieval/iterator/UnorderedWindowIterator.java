// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public class UnorderedWindowIterator extends ExtentConjunctionIterator {

  int width;
  private ScoringContext cachedContext = null;

  /**
   * Creates a new instance of UnorderedWindowIterator
   */
  public UnorderedWindowIterator(NodeParameters parameters, ExtentIterator[] evIterators) throws IOException {
    super(parameters, evIterators);
    this.width = (int) parameters.get("default", -1);
    syncTo(0);
  }

  @Override
  public void loadExtents(ScoringContext c) {
    // get the document
    long document = c.document;

    if (c.equals(cachedContext)) {
      assert (this.extentCache.getDocument() == c.document);
      return; // we already have it computed
    }
    // set current context as cached
    if (cachedContext == null) cachedContext = c.getPrototype();
    else cachedContext.setFrom(c);

    // reset the extentCache
    extentCache.reset();
    extentCache.setDocument(document);

    // if we're done - quit now 
    //  -- (leaving extentCache object empty just in cast someone asks for them.)
    if (isDone()) {
      return;
    }

    ExtentArrayIterator[] arrayIterators;
    int maximumPosition = 0;
    int minimumPosition = Integer.MAX_VALUE;

    // someday this will be a heap/priorityQueue for the overlapping case
    arrayIterators = new ExtentArrayIterator[iterators.length];

    for (int i = 0; i < iterators.length; i++) {
      if (iterators[i].isDone()
              || !iterators[i].hasMatch(document)) {
        // we can not load any extentCache if the iterator is done - or is at the wrong document.
        return;
      }

      arrayIterators[i] = new ExtentArrayIterator(((ExtentIterator) iterators[i]).extents(c));

      if (arrayIterators[i].isDone()) {
        // if this document does not have any extentCache we can not load any extentCache
        return;
      }

      minimumPosition = Math.min(arrayIterators[i].currentBegin(), minimumPosition);
      maximumPosition = Math.max(arrayIterators[i].currentEnd(), maximumPosition);
    }


    do {
      boolean match = (maximumPosition - minimumPosition <= width) || (width == -1);
      // try to emit an extent here, but only if the width is small enough
      if (match) {
        extentCache.add(minimumPosition, maximumPosition);
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
