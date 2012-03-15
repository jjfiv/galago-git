// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 *
 * @author trevor
 */
public class ScaleIterator extends TransformIterator {

  double weight;

  public ScaleIterator(NodeParameters parameters, MovableScoreIterator iterator) throws IllegalArgumentException {
    super(iterator);
    weight = parameters.get("default", 1.0);
  }
  
  public double score() {
    return weight * ((ScoreIterator)iterator).score();
  }

  public double maximumScore() {
    return ((ScoreIterator)iterator).maximumScore();
  }

  public double minimumScore() {
    return ((ScoreIterator)iterator).minimumScore();
  }
}
