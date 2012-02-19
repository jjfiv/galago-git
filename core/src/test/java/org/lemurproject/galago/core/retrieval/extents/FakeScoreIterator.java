// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ScoreValueIterator;

/**
 *
 * @author trevor
 */
public class FakeScoreIterator implements ScoreValueIterator {

  int[] docs;
  double[] scores;
  int index;
  ScoringContext context;

  public FakeScoreIterator(int[] docs, double[] scores) {
    this.docs = docs;
    this.scores = scores;
    this.index = 0;
  }

  public int currentCandidate() {
    if (index < docs.length) {
      return docs[index];
    } else {
      return Integer.MAX_VALUE;
    }

  }

  public boolean hasMatch(int document) {
    if (isDone()) {
      return false;
    } else {
      return document == docs[index];
    }
  }

  public boolean moveTo(int document) throws IOException {
    while (!isDone() && document > docs[index]) {
      index++;
    }
    return (hasMatch(document));
  }

  public void movePast(int document) throws IOException {
    while (!isDone() && document >= docs[index]) {
      index++;
    }
  }

  public double score() {
    return score(context);
  }

  public double score(ScoringContext dc) {
    if (docs[index] == dc.document) {
      return scores[index];
    }
    return 0;
  }

  public boolean isDone() {
    return index >= docs.length;
  }

  public void reset() {
    index = 0;
  }

  public double maximumScore() {
    return Double.POSITIVE_INFINITY;
  }

  public double minimumScore() {
    return Double.NEGATIVE_INFINITY;
  }

  public TObjectDoubleHashMap<String> parameterSweepScore() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public ScoringContext getContext() {
    return context;
  }

  public void setContext(ScoringContext context) {
    this.context = context;
  }

  public boolean next() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public long totalEntries() {
    return docs.length;
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

  public String getEntry() throws IOException {
    return currentCandidate() + "," + score();
  }
}
