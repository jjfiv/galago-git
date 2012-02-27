// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor, sjh, irmarc
 */
public class ScoreCombinationIterator extends DisjunctionIterator implements MovableScoreIterator {

  protected double[] weights;
  protected MovableScoreIterator[] scoreIterators;
  protected boolean done;
  protected boolean printing;

  public ScoreCombinationIterator(Parameters globalParams, NodeParameters parameters,
          MovableScoreIterator[] childIterators) {
    super(childIterators);

    weights = new double[childIterators.length];
    double weightSum = 0.0;

    for (int i = 0; i < weights.length; i++) {
      weights[i] = parameters.get(Integer.toString(i), 1.0);
      weightSum += weights[i];
    }
    printing = parameters.get("print", false);

    // if weights are to be normalized:
    if (parameters.get("norm", globalParams.get("norm", true))) {
      for (int i = 0; i < weights.length; i++) {
        weights[i] = weights[i] / weightSum;
      }
    }

    this.scoreIterators = childIterators;
  }

  @Override
  public double score() {
    double total = 0;
    for (int i = 0; i < scoreIterators.length; i++) {
      double score = scoreIterators[i].score();
      total += weights[i] * score;
    }
    return total;
  }

  @Override
  public double score(ScoringContext dc) {
    double total = 0;
    for (int i = 0; i < scoreIterators.length; i++) {
      double score = scoreIterators[i].score(dc);
      total += weights[i] * score;
    }
    return total;
  }

  @Override
  public double minimumScore() {
    double min = 0;
    for (int i = 0; i < scoreIterators.length; i++) {
      min += weights[i] * scoreIterators[i].minimumScore();
    }
    return min;
  }

  @Override
  public double maximumScore() {
    double max = 0;
    for (int i = 0; i < scoreIterators.length; i++) {
      max += weights[i] * scoreIterators[i].maximumScore();
    }
    return max;
  }

  @Override
  public String getEntry() throws IOException {
    return this.currentCandidate() + " " + this.score();
  }

  @Override
  public void setContext(ScoringContext context) {
    // do nothing by default
  }
}
