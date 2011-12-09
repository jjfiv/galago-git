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

  public SynonymIterator(NodeParameters parameters, ExtentValueIterator[] iterators) throws IOException {
    super(parameters, iterators);
    moveTo(0);
  }

  public void loadExtents() {
    if (activeIterators.size() == 0) {
      return;
    }
    ExtentValueIterator iter = activeIterators.poll();
    document = iter.currentCandidate();
    extents.setDocument(document);

    // get all the iteators that point to this intID
    ArrayList<ExtentValueIterator> useable = new ArrayList<ExtentValueIterator>();
    while (activeIterators.size() > 0 && activeIterators.peek().currentCandidate() == document) {
      useable.add(activeIterators.poll());
    }
    useable.add(iter);

    // make a priority queue of these ExtentArrayIterators
    PriorityQueue<ExtentArrayIterator> arrayIterators = new PriorityQueue<ExtentArrayIterator>();
    for (ExtentValueIterator iterator : useable) {
      arrayIterators.offer(new ExtentArrayIterator(iterator.extents()));
    }
    while (arrayIterators.size() > 0) {
      ExtentArrayIterator top = arrayIterators.poll();
      extents.add(top.currentBegin(), top.currentEnd());

      if (top.next()) {
        arrayIterators.offer(top);
      }
    }

    // put back the ones we used
    for (ExtentValueIterator i : useable) {
      if (!i.isDone()) {
        activeIterators.offer(i);
      }
    }
  }
}
