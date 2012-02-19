// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * #threshold: raw=[-]x.xx ( PriorReader ScoreIterator ) 
 * #threshold: prob=0.xx ( PriorReader ScoreIterator ) 
 * #threshold: logprob=-x.xx ( PriorReader ScoreIterator ) 
 *
 * @author sjh
 */
public class ThresholdIterator implements IndicatorIterator, ContextualIterator {

  ScoringContext context;
  ScoringContext hasMatchContext;
  ScoreValueIterator iterator;
  int document;
  double threshold;
  boolean shareNodes;

  public ThresholdIterator(Parameters globalParams, NodeParameters parameters, ScoreValueIterator scorer) {
    this.iterator = scorer;
    this.document = scorer.currentCandidate();
    this.shareNodes = globalParams.get("shareNodes", false);

    if (parameters.containsKey("raw")) {
      this.threshold = parameters.getDouble("raw");
    } else if (parameters.containsKey("prob")) {
      this.threshold = parameters.getDouble("prob");
      assert this.threshold < 0;
    } else if (parameters.containsKey("logprob")) {
      this.threshold = parameters.getDouble("logprob");
      assert this.threshold < 0;
    } else if (parameters.containsKey("raw")) {
      this.threshold = globalParams.get("raw", 0.0);
    } else if (parameters.containsKey("prob")) {
      this.threshold = globalParams.get("prob", 0.0);
      assert this.threshold < 0;
    } else if (parameters.containsKey("logprob")) {
      this.threshold = globalParams.get("logprob", 0.0);
      assert this.threshold < 0;
    } else {
      throw new RuntimeException("#threshold operator requires a thresholding parameter: [raw|prob|logprob]");
    }

    this.hasMatchContext = new ScoringContext();
  }

  public int currentCandidate() {
    return document;
  }

  public void reset() throws IOException {
    iterator.reset();
    this.document = iterator.currentCandidate();
  }

  public boolean isDone() {
    return iterator.isDone();
  }

  public boolean hasMatch(int identifier) {
    // needs to check the score against a threshold.
    hasMatchContext.document = document;
    hasMatchContext.length = 100; // need a better method.
    return ((this.document == identifier)
            && (iterator.score(hasMatchContext) >= threshold));
  }

  public boolean next() throws IOException {
    return moveTo(document + 1);
  }

  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  public boolean moveTo(int identifier) throws IOException {
    iterator.moveTo(identifier);
    document = iterator.currentCandidate();
    if (!shareNodes) {
      while (!isDone() && !hasMatch(document)) {
        iterator.movePast(document);
        document = iterator.currentCandidate();
      }
    }
    return !isDone();
  }

  public void setContext(ScoringContext context) {
    this.context = context;
  }

  public ScoringContext getContext() {
    return this.context;
  }

  public String getEntry() throws IOException {
    throw new UnsupportedOperationException("Threshold nodes don't have singular values");
  }

  public long totalEntries() {
    return iterator.totalEntries();
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
}
