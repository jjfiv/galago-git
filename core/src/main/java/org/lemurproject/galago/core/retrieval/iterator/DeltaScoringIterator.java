// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 * Defines an interface for delta-score model operation. If all the used leaf
 * iterators implement this interface, then we use delta scoring, which tends to
 * be much faster than tree-based scoring.
 *
 * @author irmarc, sjh
 */
public interface DeltaScoringIterator extends ScoreIterator {
  
  /**
   * The scoring method to use if scoring via delta functions. This score will
   * minutely shift the running total towards the proper score.
   */
  public double deltaScore(ScoringContext c);
   
  /**
   * Returns the weight the iterator uses in delta scoring.
   *
   * @return
   */
  public double getWeight();

  /**
   * Modifies the runningScore of the EarlyTerminationScoringContext by the largest amount
   * possible for this iterator. This is primarily used when determining the
   * scoring quorum.
   */
  public double maximumDifference();

  /**
   * Returns the maximum score that can be produced by the iterator
   *  -- weights the score using parameter 'w'
   * 
   * @return weighted maximum score
   */
  public double maximumWeightedScore();

  /**
   * Returns the minimum score that can be produced by the iterator
   *  -- weights the score using parameter 'w'
   * 
   * @return weighted minimum score
   */
  public double minimumWeightedScore();
  
  
  public double collectionFrequency();
}
