/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class MinCountIterator extends ConjunctionIterator implements CountIterator {

  private final NodeParameters nodeParams;
  private final CountIterator[] countIterators;

  public MinCountIterator(NodeParameters np, CountIterator[] countIterators) {
    super(np, countIterators);
    this.countIterators = countIterators;
    this.nodeParams = np;
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int count() {
    int count = Integer.MAX_VALUE;
    for (CountIterator countItr : countIterators) {
      count = Math.min(count, countItr.count());
    }
    count = (count == Integer.MAX_VALUE)? 0 : count;
    return count;
  }

  @Override
  public int maximumCount() {
    int maxCount = Integer.MAX_VALUE;
    for (CountIterator countItr : countIterators) {
      maxCount = Math.min(maxCount, countItr.maximumCount());
    }
    maxCount = (maxCount == Integer.MAX_VALUE)? 0 : maxCount;
    return maxCount;
  }

  @Override
  public String getValueString() throws IOException {
    return "NO";
  }
}
