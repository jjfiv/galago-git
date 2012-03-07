// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;

/**
 * Defines an interface for delta-score model operation. If all the used leaf iterators
 * implement this interface, then we use delta scoring, which tends to be much faster
 * than tree-based scoring.
 * 
 * @author irmarc
 */
public interface DeltaScoringIterator extends MovableScoreIterator {
  /**
   * 
   */
  public void deltaScore();
  public void maximumDifference();
}
