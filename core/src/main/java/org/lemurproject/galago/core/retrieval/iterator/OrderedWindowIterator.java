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
public class OrderedWindowIterator extends ExtentConjunctionIterator {

    private int width;

    public OrderedWindowIterator(NodeParameters parameters, ExtentIterator[] iterators) throws IOException {
        super(parameters, iterators);
        this.width = (int) parameters.get("default", -1);
        syncTo(0);
    }

    @Override
    public void loadExtentsCommon(ScoringContext c) {

        ExtentArrayIterator[] arrayIterators;
        arrayIterators = new ExtentArrayIterator[iterators.length];
        for (int i = 0; i < iterators.length; i++) {
            if (iterators[i].isDone() || !iterators[i].hasMatch(c)) {
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

                if (width == -1) {
                    continue; // always valid setting.
                }
                if (arrayIterators[i].currentBegin() - end >= width) {
                    invalid = true;
                    break;
                }
            }

            int end = arrayIterators[arrayIterators.length - 1].currentEnd();

            // if it's a match, record it
            if (!invalid) {
                extentCache.add(begin, end);
            }

            // move the first iterator forward - we are double dipping on all other iterators.
            notDone = arrayIterators[0].next();
        }
    }
}
