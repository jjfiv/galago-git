// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

/**
 * This is base interface for all inverted lists that return count information.
 * 12/11/2010 (irmarc): Refactored
 * 2/18/2011 (irmarc): Refactored again for Galago 2.0
 *
 * @see PositionIndexIterator
 * @author trevor, irmarc
 */
public interface CountIterator extends MovableIterator {

  /**
   * Shorthand set of bytes used to quickly identify this
   * iterator. Useful for things like caching the iterator.
   * @return 
   */
   public byte[] key();
  
    /**
     * Returns the number of occurrences of this iterator's term in
     * the current identifier.
     */
    public int count();

    /**
     * Upper-bound estimate of the maximum count this iterator will produce.
     * DO NOT under-estimate, otherwise you break things.
     */
    public int maximumCount();
}
