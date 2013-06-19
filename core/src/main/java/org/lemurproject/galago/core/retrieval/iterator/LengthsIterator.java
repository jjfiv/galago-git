package org.lemurproject.galago.core.retrieval.iterator;

/**
 * Key interface for reading Lengths
 * 
 * @author jfoley
 */
public interface LengthsIterator extends BaseIterator {

    // This function returns the name of the region:
    // e.g. document, field-name, or #inside(field-name field-name)
    public byte[] getRegionBytes();

    public int getCurrentLength();

    public int getCurrentIdentifier();
    
}
