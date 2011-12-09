// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import gnu.trove.TObjectDoubleHashMap;
import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.structured.ScoringContext;

/**
 *
 * @author marc
 */
public abstract class TransformIterator implements ScoreValueIterator {

  ScoringContext context;
  ValueIterator iterator;

  public TransformIterator(ValueIterator iterator) {
    this.iterator = iterator;
  }

  public ScoringContext getContext() {
    return context;
  }

  public void setContext(ScoringContext context) {
    this.context = context;
  }

  public void reset() throws IOException {
    iterator.reset();
  }

  public boolean isDone() {
    return iterator.isDone();
  }

  public int currentCandidate() {
    return iterator.currentCandidate();
  }

  public boolean hasMatch(int identifier) {
    return iterator.hasMatch(identifier);
  }

  public boolean next() throws IOException {
    return iterator.next();
  }
  
  public boolean moveTo(int identifier) throws IOException {
    return iterator.moveTo(identifier);
  }

  public void movePast(int identifier) throws IOException {
    iterator.movePast(identifier);
  }

  public String getEntry() throws IOException {
    return iterator.getEntry();
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
