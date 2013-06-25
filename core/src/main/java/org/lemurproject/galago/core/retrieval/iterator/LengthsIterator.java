package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 * Key interface for reading Lengths
 * 
 * @author jfoley
 */
public interface LengthsIterator extends BaseIterator {
    public int length(ScoringContext c);
}
