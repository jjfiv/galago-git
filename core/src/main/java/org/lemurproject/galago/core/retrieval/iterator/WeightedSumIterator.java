/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.util.MathUtils;

/**
 *
 * @author sjh
 */
@RequiredParameters(parameters = {"norm"})
public class WeightedSumIterator extends DisjunctionIterator implements ScoreIterator {

  NodeParameters np;
  protected double[] weights;
  protected ScoreIterator[] scoreIterators;
  protected boolean done;
  protected boolean printing;

  public WeightedSumIterator(NodeParameters parameters,
          ScoreIterator[] childIterators) {
    super(childIterators);
    this.np = parameters;
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

  @Override
  public double score() {
    double[] scores = new double[scoreIterators.length];
    for (int i = 0; i < scoreIterators.length; i++) {
      scores[i] = scoreIterators[i].score();
    }

    return MathUtils.weightedLogSumExp(weights, scores);
  }

  @Override
  public double minimumScore() {
    double[] scores = new double[scoreIterators.length];
    for (int i = 0; i < scoreIterators.length; i++) {
      scores[i] = scoreIterators[i].minimumScore();
    }

    return MathUtils.weightedLogSumExp(weights, scores);
  }

  @Override
  public double maximumScore() {
    double[] scores = new double[scoreIterators.length];
    for (int i = 0; i < scoreIterators.length; i++) {
      scores[i] = scoreIterators[i].maximumScore();
    }

    return MathUtils.weightedLogSumExp(weights, scores);
  }

  @Override
  public String getValueString() throws IOException {
    return this.currentCandidate() + " " + this.score();
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = Double.toString(score());
    List<AnnotatedNode> children = new ArrayList();
    for (BaseIterator child : this.iterators) {
      children.add(child.getAnnotatedNode(c));
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
