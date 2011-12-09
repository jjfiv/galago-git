// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class OrderedWindowIterator extends ExtentConjunctionIterator {

  int width;

  /** Creates a new instance of OrderedWindowIterator */
  public OrderedWindowIterator(Parameters globalParams, NodeParameters parameters, ExtentValueIterator[] iterators) throws IOException {
    super(globalParams, parameters, iterators);
    this.width = (int) parameters.get("default", -1);
    moveTo(0);
  }
  
  public void loadExtents() {
    extents.setDocument(document);
    ExtentArrayIterator[] arrayIterators;

    arrayIterators = new ExtentArrayIterator[iterators.length];
    for (int i = 0; i < iterators.length; i++) {
      arrayIterators[i] = new ExtentArrayIterator(iterators[i].extents());
      assert !arrayIterators[i].isDone();
    }
    boolean notDone = true;
    while (notDone) {
      // find the start of the first word
      boolean invalid = false;
      int begin = arrayIterators[0].currentBegin();

      // loop over all the rest of the words
      for (int i = 1; i < arrayIterators.length; i++) {
        int end = arrayIterators[i - 1].currentEnd();

        // try to move this iterator so that it's past the end of the previous word
        assert (arrayIterators[i] != null);
        assert (!arrayIterators[i].isDone());
        while (end > arrayIterators[i].currentBegin()) {
          notDone = arrayIterators[i].next();

          // if there are no more occurrences of this word,
          // no more ordered windows are possible
          if (!notDone) {
            return;
          }
        }

        if (arrayIterators[i].currentBegin() - end >= width) {
          invalid = true;
          break;
        }
      }

      int end = arrayIterators[arrayIterators.length - 1].currentEnd();

      // if it's a match, record it
      if (!invalid) {
        extents.add(begin, end);
      }

      // move the first iterator forward - we are double dipping on all other iterators.
      notDone = arrayIterators[0].next();
    }
  }
}
