// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.ScoringContext;

/**
 *
 * @author trevor
 */
public class ScaleIterator extends TransformIterator {

  double weight;

  public ScaleIterator(NodeParameters parameters, ScoreValueIterator iterator) throws IllegalArgumentException {
    super(iterator);
    weight = parameters.get("default", 1.0);
  }
  
  public double score() {
    return weight * ((ScoreIterator)iterator).score();
  }

  public double score(ScoringContext context) {
    return weight * ((ScoreIterator)iterator).score(context);
  }

  public double maximumScore() {
    return ((ScoreIterator)iterator).maximumScore();
  }

  public double minimumScore() {
    return ((ScoreIterator)iterator).minimumScore();
  }
}
