// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
  public boolean hasMatch(ScoringContext context) {
    if (isDone()) {
      return false;
    } else {
      return context.document == docs[index];
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
  public double score(ScoringContext c) {
    if (!isDone() && docs[index] == c.document) {
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
  public long totalEntries() {
    return docs.length;
  }

  @Override
  public String getValueString(ScoringContext sc) throws IOException {
    return currentCandidate() + "," + score(sc);
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
    boolean atCandidate = hasMatch(c);
    String returnValue = Double.toString(score(c));
    List<AnnotatedNode> children = Collections.emptyList();

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
