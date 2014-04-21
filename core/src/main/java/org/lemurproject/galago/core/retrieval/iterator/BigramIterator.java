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

  private ScoringContext cachedContext = null;

  public BigramIterator(NodeParameters parameters, ExtentIterator[] iterators) throws IOException {
    super(parameters, iterators);
    int width = (int) parameters.get("default", -1);
    assert(width == 1):"Bigram iterator can only work with width 1";
    syncTo(0);
  }

  @Override
  public void loadExtents(ScoringContext c) {
    // get the document
    final long document = c.document;

    if (c.equals(cachedContext)) {
      assert (this.extentCache.getDocument() == document);
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

    assert (iterators.length == 2);

    final ExtentIterator leftTerm = (ExtentIterator) iterators[0];
    final ExtentIterator rightTerm = (ExtentIterator) iterators[1];

    if(leftTerm.isDone() || rightTerm.isDone() || !leftTerm.hasMatch(document) || !rightTerm.hasMatch(document)) {
      return;
    }

    final int leftCount = leftTerm.count(c);
    final int rightCount = rightTerm.count(c);

    if(leftCount == 0 || rightCount == 0) {
      return;
    }

    final ExtentArrayIterator left = new ExtentArrayIterator(leftTerm.extents(c));
    final ExtentArrayIterator right = new ExtentArrayIterator(rightTerm.extents(c));

    // redundant?
    if(left.isDone() || right.isDone())
      return;

    boolean hasNext = true;
    while(hasNext) {
      final int lhs = left.currentEnd();
      final int rhs = right.currentBegin();

      if(lhs < rhs) {
        hasNext = left.next();
      } else if(lhs > rhs) {
        hasNext = right.next();
      } else { // equal; matched
        extentCache.add(left.currentBegin(), right.currentEnd());
        hasNext = left.next();
      }
    }
  }
}
