// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 * This class is meant to be a base class for many kinds of
 * iterators that require at least one of their children to be
 * present in the intID for a match to happen.  This class
 * will call loadExtents once for each id that has a
 * match on any child iterator.
 * 
 * @author trevor
 */
public abstract class ExtentDisjunctionIterator extends ExtentCombinationIterator {

  protected PriorityQueue<ExtentValueIterator> activeIterators;
  protected int document;

  public ExtentDisjunctionIterator(NodeParameters parameters, ExtentValueIterator[] iterators) throws IOException {
    this.iterators = iterators;
    this.activeIterators = new PriorityQueue<ExtentValueIterator>(iterators.length);

    this.extents = new ExtentArray();
    this.document = 0;

    for (ExtentValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
        this.activeIterators.add(iterator);
      }
    }
  }

  public int currentCandidate() {
    return document;
  }

  public boolean isDone() {
    return activeIterators.size() == 0;
  }

  public void reset() throws IOException {
    activeIterators.clear();
    for (ExtentValueIterator iterator : iterators) {
      iterator.reset();
      if (!iterator.isDone()) {
        activeIterators.add(iterator);
      }
    }
    moveTo(0);
  }

  public long totalEntries() {
    long max = 0;
    for (ValueIterator iterator : activeIterators) {
      max = Math.max(max, iterator.totalEntries());
    }
    return max;
  }

  public boolean moveTo(int identifier) throws IOException {
    // move iterators to identifier
    while (activeIterators.size() > 0
            && activeIterators.peek().currentCandidate() < identifier) {
      ExtentValueIterator iter = activeIterators.poll();
      iter.moveTo(identifier);
      if (!iter.isDone()) {
        activeIterators.offer(iter);
      }
    }

    document = activeIterators.size() > 0 ? activeIterators.peek().currentCandidate() : Integer.MAX_VALUE;
    extents.reset();
    if (activeIterators.size() > 0 && activeIterators.peek().hasMatch(document)) {
      loadExtents();
    }
    return !isDone();
  }
}
