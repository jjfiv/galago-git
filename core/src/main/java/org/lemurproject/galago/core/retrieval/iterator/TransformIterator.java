// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 *
 * @author irmarc
 */
public abstract class TransformIterator implements BaseIterator {

  public ScoringContext context;
  public BaseIterator iterator;

  public TransformIterator(BaseIterator iterator) {
    this.iterator = iterator;
  }

  @Override
  public void setContext(ScoringContext context) {
    this.context = context;
    iterator.setContext(context);
  }

  @Override
  public ScoringContext getContext() {
    return this.context;
  }

  @Override
  public void reset() throws IOException {
    iterator.reset();
  }

  @Override
  public boolean isDone() {
    return iterator.isDone();
  }

  @Override
  public boolean hasAllCandidates() {
    return iterator.hasAllCandidates();
  }

  @Override
  public int currentCandidate() {
    return iterator.currentCandidate();
  }

  @Override
  public boolean hasMatch(int identifier) {
    return iterator.hasMatch(identifier);
  }

  @Override
  public void syncTo(int identifier) throws IOException {
    iterator.syncTo(identifier);
  }

  @Override
  public void movePast(int identifier) throws IOException {
    iterator.movePast(identifier);
  }

  @Override
  public String getValueString() throws IOException {
    return iterator.getValueString();
  }

  @Override
  public long totalEntries() {
    return iterator.totalEntries();
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
    return currentCandidate() - other.currentCandidate();
  }
}
