// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

/**
 * This is base interface for all inverted lists that return count information.
 *
 * @author trevor, irmarc, sjh
 */
public interface CountIterator extends BaseIterator {
    /**
     * Returns the number of occurrences of this iterator's term in
     * the current identifier.
     */
    public int count();
}
