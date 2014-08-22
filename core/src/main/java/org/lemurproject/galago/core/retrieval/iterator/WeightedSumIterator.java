/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.utility.MathUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    if(childIterators.length == 0) {
      throw new RuntimeException("bad construction!");
    }

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
  public double score(ScoringContext c) {
    double[] scores = new double[scoreIterators.length];
    for (int i = 0; i < scoreIterators.length; i++) {
      scores[i] = scoreIterators[i].score(c);
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
  public String getValueString(ScoringContext c) throws IOException {
    return this.currentCandidate() + " " + this.score(c);
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = Double.toString(score(c));
    List<AnnotatedNode> children = new ArrayList();
    for (BaseIterator child : this.iterators) {
      children.add(child.getAnnotatedNode(c));
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
