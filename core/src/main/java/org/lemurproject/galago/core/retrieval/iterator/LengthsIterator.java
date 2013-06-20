package org.lemurproject.galago.core.retrieval.iterator;

/**
 * Key interface for reading Lengths
 * 
 * @author jfoley
 */
public interface LengthsIterator extends BaseIterator {
    public int getCurrentLength();

    public int getCurrentIdentifier();
    
}
