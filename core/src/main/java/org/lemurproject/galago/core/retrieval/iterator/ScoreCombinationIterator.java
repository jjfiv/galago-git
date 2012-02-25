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
public class ScoreCombinationIterator implements ScoreValueIterator {

  protected double[] weights;
  protected ScoreValueIterator[] iterators;
  protected boolean done;
  protected boolean printing;

  public ScoreCombinationIterator(Parameters globalParams, NodeParameters parameters,
          ScoreValueIterator[] childIterators) {

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

    this.iterators = childIterators;
  }

  public int currentCandidate() {
    return MoveIterators.findMinimumDocument(iterators);
  }

  public boolean atCandidate(int identifier) {
    return MoveIterators.anyHasMatch(iterators, identifier);
  }

  public boolean isDone() {
    for (ValueIterator iterator : iterators) {
      if (!iterator.isDone()) {
        return false;
      }
    }
    return true;
  }

  public boolean next() throws IOException {
    return moveTo(currentCandidate() + 1);
  }

  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  public boolean moveTo(int identifier) throws IOException {
    for (ValueIterator iterator : iterators) {
      iterator.moveTo(identifier);
    }
    return !isDone();
  }

  public double score() {
    double total = 0;

    for (int i = 0; i < iterators.length; i++) {
      double score = iterators[i].score();
      total += weights[i] * score;
    }
    return total;
  }

  public double score(ScoringContext dc) {
    double total = 0;

    for (int i = 0; i < iterators.length; i++) {
      double score = iterators[i].score(dc);
      total += weights[i] * score;
    }
    return total;
  }

  public void setContext(ScoringContext context) {
    // This is done when the children are constructed
  }

  public ScoringContext getContext() {
    return iterators[0].getContext();
  }

  public int compareTo(ValueIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return currentCandidate() - other.currentCandidate();
  }

  public void reset() throws IOException {
    for (StructuredIterator iterator : iterators) {
      iterator.reset();
    }
  }

  public double minimumScore() {
    double min = 0;
    for (int i = 0; i < iterators.length; i++) {
      min += weights[i] * iterators[i].minimumScore();
    }
    return min;
  }

  public double maximumScore() {
    double max = 0;
    for (int i = 0; i < iterators.length; i++) {
      max += weights[i] * iterators[i].maximumScore();
    }
    return max;
  }

  public long totalEntries() {
    long max = 0;
    for (ValueIterator iterator : iterators) {
      max = Math.max(max, iterator.totalEntries());
    }
    return max;
  }

  public String getEntry() throws IOException {
    throw new UnsupportedOperationException("Score combine nodes don't have singular values");
  }
}
