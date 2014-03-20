/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 *
 * @author sjh
 */
public class BigramIterator extends ExtentConjunctionIterator {

  private int width;
    private ScoringContext cachedContext = null;

  public BigramIterator(NodeParameters parameters, ExtentIterator[] iterators) throws IOException {
    super(parameters, iterators);
    this.width = (int) parameters.get("default", -1);
      assert(this.width == 1):"Bigram iterator can only work with width 1";
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





//     System.out.println(document);

    // reset the extentCache
    extentCache.reset();
    extentCache.setDocument(document);

    // if we're done - quit now 
    //  -- (leaving extentCache object empty just in cast someone asks for them.)
    if (isDone()) {
      return;
    }

    assert (iterators.length == 2);

    ExtentArrayIterator[] arrayIterators;
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

    }

    boolean notDone = true;
    while (notDone) {
      // find the start of the first word
      boolean invalid = false;
      int begin = arrayIterators[0].currentBegin();
      int end = arrayIterators[0].currentEnd();

        // try to move this iterator so that it's past the end of the previous word
        assert (arrayIterators[1] != null);
        assert (!arrayIterators[1].isDone());
        while (end > arrayIterators[1].currentBegin()) {
          notDone = arrayIterators[1].next();

          // if there are no more occurrences of this word,
          // no more ordered windows are possible
          if (!notDone) {
            return;
          }
        }

        int begin2 = arrayIterators[1].currentBegin();
        int end2 = arrayIterators[1].currentEnd();

        if (arrayIterators[1].currentBegin() - end >= 1) {
          invalid = true;
        }

      // if it's a match, record it
      if (!invalid) {
        extentCache.add(begin, end2);
      }

      // move the first iterator forward - we are double dipping on all other iterators.
      notDone = arrayIterators[0].next();
    }
  }
}
