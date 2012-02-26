// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 * The internal iterator is expected to be an indicator
 * iterator. This performs a transform into the "score space"
 * by emitting a boost score (beta) iff the indicator is on.
 * @author irmarc
 */
public class BoostingIterator extends TransformIterator {

  double beta;

  public BoostingIterator(NodeParameters p, IndicatorIterator inner) {
    super((ValueIterator) inner);
    beta = p.get("beta", 0.5);
  }

  public double score() {
    if(atCandidate(context.document) 
            && ((IndicatorIterator) iterator).indicator(context.document)){
      return beta;
    } else {
      return 0.0;
    }
  }

  public double score(ScoringContext context) {
    if(atCandidate(context.document) 
            && ((IndicatorIterator) iterator).indicator(context.document)){
      return beta;
    } else {
      return 0.0;
    }
  }

  public double maximumScore() {
    return beta;
  }

  public double minimumScore() {
    return 0.0;
  }
}
