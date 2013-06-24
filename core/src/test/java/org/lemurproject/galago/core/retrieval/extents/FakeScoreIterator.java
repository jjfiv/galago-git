// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class FakeScoreIterator implements ScoreIterator {

  int[] docs;
  double[] scores;
  double defaultScore;
  int index;
  ScoringContext context;

  public FakeScoreIterator(int[] docs, double[] scores) {
    this(docs, scores, 0);
  }

  public FakeScoreIterator(int[] docs, double[] scores, double defaultScore) {
    this.docs = docs;
    this.scores = scores;
    this.index = 0;
    this.defaultScore = defaultScore;
  }

  @Override
  public long currentCandidate() {
    if (index < docs.length) {
      return docs[index];
    } else {
      return Long.MAX_VALUE;
    }

  }

  @Override
  public boolean hasMatch(long document) {
    if (isDone()) {
      return false;
    } else {
      return document == docs[index];
    }
  }

  @Override
  public void syncTo(long document) throws IOException {
    while (!isDone() && document > docs[index]) {
      index++;
    }
  }

  @Override
  public void movePast(long document) throws IOException {
    while (!isDone() && document >= docs[index]) {
      index++;
    }
  }

  @Override
  public double score() {
    if (!isDone() && docs[index] == context.document) {
      return scores[index];
    }
    return defaultScore;
  }

  @Override
  public boolean isDone() {
    return (index >= docs.length);
  }

  @Override
  public void reset() {
    index = 0;
  }

  @Override
  public double maximumScore() {
    return Double.POSITIVE_INFINITY;
  }

  @Override
  public double minimumScore() {
    return Double.NEGATIVE_INFINITY;
  }

  @Override
  public void setContext(ScoringContext context) {
    this.context = context;
  }

  @Override
  public ScoringContext getContext() {
    return context;
  }

  @Override
  public long totalEntries() {
    return docs.length;
  }

  @Override
  public int compareTo(BaseIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return Utility.compare(currentCandidate(), other.currentCandidate());
  }

  @Override
  public String getValueString() throws IOException {
    return currentCandidate() + "," + score();
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = Double.toString(score());
    List<AnnotatedNode> children = Collections.EMPTY_LIST;

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
