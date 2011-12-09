/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.util.Collection;
import org.lemurproject.galago.core.retrieval.iterator.CountValueIterator;

/**
 *
 * @author sjh
 */
public class DisjointCountsIterator extends DisjointIndexesIterator implements CountValueIterator {

  public DisjointCountsIterator(Collection<CountValueIterator> iterators){
    super((Collection) iterators);
  }
  
  public int count() {
    return ((CountValueIterator) head).count();
  }
  
}
