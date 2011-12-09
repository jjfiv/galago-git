// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;

/**
 * This is a marker interface that represents any kind of
 * iterator over an inverted list or query operator.
 * 
 * @author trevor
 */
public interface StructuredIterator {
    public void reset() throws IOException;
    public boolean isDone();
}
