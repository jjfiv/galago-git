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
    syncTo(0);
  }

  public void loadExtents() {
    // get the document
    int document = context.document;

    // check if we're already there
    if (this.extents.getDocument() == document) {
      return;
    }

    // reset the extents
    extents.reset();
    extents.setDocument(document);

    // if we're done - quit now 
    //  -- (leaving extents object empty just in cast someone asks for them.)
    if (isDone()) {
      return;
    }

    // make a priority queue of extent array iterators
    PriorityQueue<ExtentArrayIterator> arrayIterators = new PriorityQueue<ExtentArrayIterator>();
    for (MovableExtentIterator iterator : this.extentIterators) {
      if (!iterator.isDone() && iterator.hasMatch(document)) {
        arrayIterators.offer(new ExtentArrayIterator(iterator.extents()));
      }
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
