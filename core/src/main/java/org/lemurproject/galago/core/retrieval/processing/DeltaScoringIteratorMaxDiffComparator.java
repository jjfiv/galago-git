/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.utility.CmpUtil;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Sorts iterators by the maximum change in score:
 *   (weight * (max - min))
 * 
 *  Decreasing order
 * 
 * @author sjh
 */
public class DeltaScoringIteratorMaxDiffComparator implements Comparator<DeltaScoringIterator>, Serializable {

  @Override
  public int compare(DeltaScoringIterator t1, DeltaScoringIterator t2) {
    return CmpUtil.compare(t2.maximumDifference(), t1.maximumDifference());
  }
}
