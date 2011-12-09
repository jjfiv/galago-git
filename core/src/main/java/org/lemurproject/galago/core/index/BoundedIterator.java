// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;

/**
 * 
 * @author irmarc
 */
public interface BoundedIterator extends StructuredIterator {
  public long totalEntries();
}
