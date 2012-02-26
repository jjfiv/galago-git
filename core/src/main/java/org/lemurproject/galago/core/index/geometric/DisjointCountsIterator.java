/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.util.Collection;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;

/**
 *
 * @author sjh
 */
public class DisjointCountsIterator extends DisjointIndexesIterator implements MovableCountIterator {

  public DisjointCountsIterator(Collection<MovableCountIterator> iterators) {
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
}
