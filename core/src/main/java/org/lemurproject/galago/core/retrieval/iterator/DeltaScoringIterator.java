// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;

/**
 * Defines an interface for delta-score model operation. If all the used leaf
 * iterators implement this interface, then we use delta scoring, which tends to
 * be much faster than tree-based scoring.
 *
 * @author irmarc
 */
public interface DeltaScoringIterator extends MovableScoreIterator {

  public byte[] key();
  
  /**
   * The scoring method to use if scoring via delta functions. This score will
   * minutely shift the running total towards the proper score.
   */
  public void deltaScore();

  public void deltaScore(int length);
  
  public void deltaScore(int count, int length);
  
 
  /**
   * Returns the weight the iterator uses in delta scoring.
   *
   * @return
   */
  public double getWeight();

  /**
   * Modifies the runningScore of the DeltaScoringContext by the largest amount
   * possible for this iterator. This is primarily used when determining the
   * scoring quorum.
   */
  public void maximumDifference();

  /**
   * This is an unfortunate consequence of Java not being able to support traits
   * or static methods in interfaces. This should be a static method, but the
   * language being what it is, there's no clean way to enforce that besides
   * making it a regular method to implement to fulfill the DeltaScoringIterator
   * contract.
   *
   * Takes in the potential scores and aggregates them to make the final
   * startingPotential score. In some cases, nothing needs to be done.
   */
  public void aggregatePotentials(DeltaScoringContext ctx);
}
