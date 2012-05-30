/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.Collection;
import org.lemurproject.galago.core.retrieval.iterator.MovableExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author sjh
 */
public class DisjointExtentsIterator extends DisjointIndexesIterator implements MovableExtentIterator, MovableCountIterator {

  public DisjointExtentsIterator(Collection<MovableExtentIterator> iterators) {
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
  public ExtentArray extents() throws IOException {
    return ((MovableExtentIterator) head).extents();
  }

  @Override
  public ExtentArray getData() throws IOException {
    return ((MovableExtentIterator) head).getData();
  }
}
