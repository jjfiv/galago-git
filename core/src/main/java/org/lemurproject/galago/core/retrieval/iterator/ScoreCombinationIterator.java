// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;

/**
 *
 * @author trevor, sjh, irmarc
 */
@RequiredParameters(parameters = {"norm"})
public class ScoreCombinationIterator extends DisjunctionIterator implements ScoreIterator {

  NodeParameters np;
  protected double[] weights;
  protected ScoreIterator[] scoreIterators;
  protected boolean done;
  protected boolean printing;

  public ScoreCombinationIterator(NodeParameters parameters,
          ScoreIterator[] childIterators) {
    super(childIterators);

    assert (childIterators.length > 0) : "#combine nodes must have more than 1 child.";

    this.np = parameters;

    this.scoreIterators = childIterators;

    weights = new double[childIterators.length];
    printing = parameters.get("print", false);

    double weightSum = 0.0;

    for (int i = 0; i < weights.length; i++) {
      weights[i] = parameters.get(Integer.toString(i), 1.0);
      weightSum += weights[i];
    }

    // if weights are to be normalized:
    if (parameters.get("norm", true)) {
      // we can't normalize using a negative value - causes problems in the learner.
      if (weightSum <= 0.0) {
        Logger.getLogger(this.getClass().getSimpleName()).log(Level.INFO, "WeightSum is negative : {0} : Not normalizing.", weightSum);
      } else {
        for (int i = 0; i < weights.length; i++) {
          weights[i] = weights[i] / weightSum;
        }
      }
    }
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
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Double.toString(score());
    List<AnnotatedNode> children = new ArrayList();
    for (BaseIterator child : this.iterators) {
      children.add(child.getAnnotatedNode());
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
