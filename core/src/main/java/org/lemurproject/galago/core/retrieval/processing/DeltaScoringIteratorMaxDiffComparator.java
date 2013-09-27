/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Comparator;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Sorts iterators by the maximum change in score:
 *   (weight * (max - min))
 * 
 *  Decreasing order
 * 
 * @author sjh
 */
public class DeltaScoringIteratorMaxDiffComparator implements Comparator<DeltaScoringIterator> {

  @Override
  public int compare(DeltaScoringIterator t1, DeltaScoringIterator t2) {
    return Utility.compare(t2.maximumDifference(), t1.maximumDifference());
  }
}
