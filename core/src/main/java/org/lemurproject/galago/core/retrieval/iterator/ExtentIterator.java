// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.util.ExtentArray;

/**
 * This extends a identifier-ordered navigable count iterator by returning
 * arrays of extents, each of which is a position range (start - end), docid, and
 * weight.
 * 
 * @author trevor, irmarc
 */
public interface ExtentIterator extends DataIterator<ExtentArray> {
    public ExtentArray extents();
}
