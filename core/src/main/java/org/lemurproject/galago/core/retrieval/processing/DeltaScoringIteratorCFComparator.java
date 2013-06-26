/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Comparator;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Sorts sentinels by increasing collection frequency
 * 
 * @author sjh, 
 */
public class DeltaScoringIteratorCFComparator implements Comparator<DeltaScoringIterator> {

  @Override
  public int compare(DeltaScoringIterator t1, DeltaScoringIterator t2) {
    return Utility.compare(t1.collectionFrequency(), t2.collectionFrequency());
  }
}
