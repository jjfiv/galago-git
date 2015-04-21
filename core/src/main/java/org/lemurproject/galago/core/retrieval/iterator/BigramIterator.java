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

    public BigramIterator(NodeParameters parameters, ExtentIterator[] iterators) throws IOException {
        super(parameters, iterators);
        int width = (int) parameters.get("default", -1);
        assert (width == 1) : "Bigram iterator can only work with width 1";
        syncTo(0);
    }

    @Override
    public void loadExtentsCommon(ScoringContext c) {

        assert (iterators.length == 2);

        final ExtentIterator leftTerm = (ExtentIterator) iterators[0];
        final ExtentIterator rightTerm = (ExtentIterator) iterators[1];

        if (leftTerm.isDone() || rightTerm.isDone() || !leftTerm.hasMatch(c) || !rightTerm.hasMatch(c)) {
            return;
        }

        final int leftCount = leftTerm.count(c);
        final int rightCount = rightTerm.count(c);

        if (leftCount == 0 || rightCount == 0) {
            return;
        }

        final ExtentArrayIterator left = new ExtentArrayIterator(leftTerm.extents(c));
        final ExtentArrayIterator right = new ExtentArrayIterator(rightTerm.extents(c));

        // redundant?
        if (left.isDone() || right.isDone()) {
            return;
        }

        boolean hasNext = true;
        while (hasNext) {
            final int lhs = left.currentEnd();
            final int rhs = right.currentBegin();

            if (lhs < rhs) {
                hasNext = left.next();
            } else if (lhs > rhs) {
                hasNext = right.next();
            } else { // equal; matched
                extentCache.add(left.currentBegin(), right.currentEnd());
                hasNext = left.next();
            }
        }
    }

}
