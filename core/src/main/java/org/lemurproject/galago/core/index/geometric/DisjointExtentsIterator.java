/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.util.Collection;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentValueIterator;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author sjh
 */
public class DisjointExtentsIterator extends DisjointIndexesIterator implements ExtentValueIterator, CountValueIterator {

  public DisjointExtentsIterator(Collection<ExtentValueIterator> iterators) {
    super((Collection) iterators);
  }

  @Override
  public int count() {
    return ((CountValueIterator) head).count();
  }

  @Override
  public int maximumCount() {
    return ((CountValueIterator) head).maximumCount();
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
