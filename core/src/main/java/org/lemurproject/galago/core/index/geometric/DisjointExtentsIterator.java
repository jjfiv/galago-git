/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.util.Collection;
import org.lemurproject.galago.core.retrieval.iterator.ExtentValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author sjh
 */
public class DisjointExtentsIterator extends DisjointIndexesIterator implements ExtentValueIterator, MovableCountIterator {

  public DisjointExtentsIterator(Collection<ExtentValueIterator> iterators) {
    super((Collection) iterators);
  }

  @Override
  public int count() {
    return ((MovableCountIterator) head).count();
  }

  @Override
  public int maximumCount() {
    return ((MovableCountIterator) head).maximumCount();
  }

  @Override
  public ExtentArray extents() {
    return ((ExtentValueIterator) head).extents();
  }

  @Override
  public ExtentArray getData() {
    return ((ExtentValueIterator) head).getData();
  }
}
