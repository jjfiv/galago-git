// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;

/**
 *
 * @author trevor
 */
public class FakeScoreIterator implements MovableScoreIterator {

  int[] docs;
  double[] scores;
  int index;
  ScoringContext context;

  public FakeScoreIterator(int[] docs, double[] scores) {
    this.docs = docs;
    this.scores = scores;
    this.index = 0;
  }

  @Override
  public int currentCandidate() {
    if (index < docs.length) {
      return docs[index];
    } else {
      return Integer.MAX_VALUE;
    }

  }

  @Override
  public boolean atCandidate(int document) {
    if (isDone()) {
      return false;
    } else {
      return document == docs[index];
    }
  }

  @Override
  public void moveTo(int document) throws IOException {
    while (!isDone() && document > docs[index]) {
      index++;
    }
  }

  @Override
  public void movePast(int document) throws IOException {
    while (!isDone() && document >= docs[index]) {
      index++;
    }
  }

  @Override
  public double score() {
    return score(context);
  }

  @Override
  public double score(ScoringContext dc) {
    if (docs[index] == dc.document) {
      return scores[index];
    }
    return 0;
  }

  @Override
  public boolean isDone() {
    return index >= docs.length;
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
  public void next() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long totalEntries() {
    return docs.length;
  }

  @Override
  public int compareTo(MovableIterator other) {
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

  @Override
  public String getEntry() throws IOException {
    return currentCandidate() + "," + score();
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }
}
