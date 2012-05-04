/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;

/**
 *
 * @author sjh
 */
@RequiredParameters(parameters = {"norm"})
public class WeightedSumIterator extends DisjunctionIterator implements MovableScoreIterator {

  protected double[] weights;
  protected MovableScoreIterator[] scoreIterators;
  protected boolean done;
  protected boolean printing;
  protected ScoringContext context;

  public WeightedSumIterator(NodeParameters parameters,
          MovableScoreIterator[] childIterators) {
    super(childIterators);

    this.scoreIterators = childIterators;

    weights = new double[childIterators.length];
    double weightSum = 0.0;

    for (int i = 0; i < weights.length; i++) {
      weights[i] = parameters.get(Integer.toString(i), 1.0);
      weightSum += weights[i];
    }
    printing = parameters.get("print", false);

    // if weights are to be normalized:
    if (parameters.get("norm", false)) {
      for (int i = 0; i < weights.length; i++) {
        weights[i] = weights[i] / weightSum;
      }
    }
  }

  /**
   * Computes the weighted average of scores: -> log( w1 * exp(score[0]) + w1 *
   * exp(score[1]) + w1 * exp(score[2]) + .. )
   *
   * to avoid underflows, we compute the equivalent expression:
   *
   * returns: maxScore + log( exp(score[0] - max) + exp(score[1] - max) +
   * exp(score[2] - max) + .. )
   */
  private double weightedLogSumExp(double[] scores) {

    // find max value - this score will dominate the final value
    double max = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < scores.length; i++) {
      max = (max < scores[i]) ? scores[i] : max;
    }

    double opt1 = 0;
    for (int i = 0; i < scores.length; i++) {
      opt1 += weights[i] * Math.exp(scores[i] - max);
    }
    opt1 = max + Math.log(opt1);

    //System.err.println( "~" + max + " " + opt1);

    //double opt2 = 0;
    //for (int i = 0; i < scores.length; i++) {
    //  opt2 += weights[i] * Math.exp(scores[i]);
    //}
    //opt2 = Math.log(opt2);

    //System.err.println(opt1 + " " + opt2);

    return opt1;
  }

  @Override
  public double score() {
    double[] scores = new double[scoreIterators.length];
    for (int i = 0; i < scoreIterators.length; i++) {
      scores[i] = scoreIterators[i].score();
    }

    return weightedLogSumExp(scores);
  }

  @Override
  public double minimumScore() {
    double[] scores = new double[scoreIterators.length];
    for (int i = 0; i < scoreIterators.length; i++) {
      scores[i] = scoreIterators[i].minimumScore();
    }

    return weightedLogSumExp(scores);
  }

  @Override
  public double maximumScore() {
    double[] scores = new double[scoreIterators.length];
    for (int i = 0; i < scoreIterators.length; i++) {
      scores[i] = scoreIterators[i].maximumScore();
    }

    return weightedLogSumExp(scores);
  }

  @Override
  public String getEntry() throws IOException {
    return this.currentCandidate() + " " + this.score();
  }

  @Override
  public void setContext(ScoringContext context) {
    this.context = context;
  }

  @Override
  public ScoringContext getContext() {
    return context;
  }
}
