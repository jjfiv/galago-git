// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * #threshold: raw=[-]x.xx ( PriorReader ScoreIterator ) 
 * #threshold: prob=0.xx ( PriorReader ScoreIterator ) 
 * #threshold: logprob=-x.xx ( PriorReader ScoreIterator ) 
 *
 * @author sjh
 */
public class ThresholdIterator extends TransformIterator implements MovableIndicatorIterator, ContextualIterator {

  double threshold;
  MovableScoreIterator scoreIterator;

  public ThresholdIterator(NodeParameters parameters, MovableScoreIterator scorer) {
    super(scorer);
    this.scoreIterator = scorer;
    
    if (parameters.containsKey("raw")) {
      this.threshold = parameters.getDouble("raw");

    } else if (parameters.containsKey("prob")) {
      this.threshold = parameters.getDouble("prob");
      assert this.threshold < 0;
    
    } else if (parameters.containsKey("logprob")) {
      this.threshold = parameters.getDouble("logprob");
      assert this.threshold < 0;
    
    } else {
      throw new RuntimeException("#threshold operator requires a thresholding parameter: [raw|prob|logprob]");
    }
  }

  
  /** note that this indicator may depends on the scoring context! **/
  @Override
  public boolean indicator(int identifier) {
    return (scoreIterator.score() >= threshold);
  }
}
