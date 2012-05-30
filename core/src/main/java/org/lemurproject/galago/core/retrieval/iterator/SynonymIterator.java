// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author trevor
 */
public class SynonymIterator extends ExtentDisjunctionIterator {

  MovableExtentIterator[] extentIterators;

  public SynonymIterator(NodeParameters parameters, MovableExtentIterator[] iterators) throws IOException {
    super(iterators);
    extentIterators = iterators;
    moveTo(0);
  }

  @Override
  public void loadExtents() {
    if (isDone()) {
      return;
    }
    int document = this.currentCandidate();
    extents.setDocument(document);

    // make a priority queue of extent array iterators
    PriorityQueue<ExtentArrayIterator> arrayIterators = new PriorityQueue<ExtentArrayIterator>();
    try {
      for (MovableExtentIterator iterator : this.extentIterators) {
        if (!iterator.isDone() && iterator.atCandidate(document)) {
          arrayIterators.offer(new ExtentArrayIterator(iterator.extents()));
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    while (arrayIterators.size() > 0) {
      ExtentArrayIterator top = arrayIterators.poll();
      extents.add(top.currentBegin(), top.currentEnd());

      if (top.next()) {
        arrayIterators.offer(top);
      }
    }
  }
}
