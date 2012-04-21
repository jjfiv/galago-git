// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * The internal iterator is expected to be an indicator
 * iterator. This performs a transform into the "score space"
 * by emitting a boost score (beta) iff the indicator is on.
 * @author irmarc
 */
public class BoostingIterator extends TransformIterator implements MovableScoreIterator {

  double beta;

  public BoostingIterator(NodeParameters p, MovableIndicatorIterator inner) {
    super(inner);
    beta = p.get("beta", 0.5);
  }

  @Override
  public double score() {
    if(atCandidate(context.document) 
            && ((IndicatorIterator) iterator).indicator(context.document)){
      return beta;
    } else {
      return 0.0;
    }
  }

  @Override
  public double maximumScore() {
    return beta;
  }

  @Override
  public double minimumScore() {
    return 0.0;
  }
}
