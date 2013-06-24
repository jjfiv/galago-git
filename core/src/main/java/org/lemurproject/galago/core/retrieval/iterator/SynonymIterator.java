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

  ExtentIterator[] extentIterators;

  public SynonymIterator(NodeParameters parameters, ExtentIterator[] iterators) throws IOException {
    super(iterators);
    extentIterators = iterators;
    syncTo(0);
  }

  public void loadExtents() {
    // get the document
    long document = context.document;

    // check if we're already there
    if (context.cachable && this.extentCache.getDocument() == document) {
      return;
    }

    // reset the extentCache
    extentCache.reset();
    extentCache.setDocument(document);

    // if we're done - quit now 
    //  -- (leaving extentCache object empty just in cast someone asks for them.)
    if (isDone()) {
      return;
    }

    // make a priority queue of extent array iterators
    PriorityQueue<ExtentArrayIterator> arrayIterators = new PriorityQueue<ExtentArrayIterator>();
    for (ExtentIterator iterator : this.extentIterators) {
      if (!iterator.isDone() && iterator.hasMatch(document)) {
        arrayIterators.offer(new ExtentArrayIterator(iterator.extents()));
      }
    }

    while (arrayIterators.size() > 0) {
      ExtentArrayIterator top = arrayIterators.poll();
      extentCache.add(top.currentBegin(), top.currentEnd());

      if (top.next()) {
        arrayIterators.offer(top);
      }
    }
  }
}
