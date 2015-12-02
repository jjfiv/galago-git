// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

import java.io.IOException;

/**
 *
 * @author irmarc
 */
public abstract class TransformIterator implements BaseIterator {

  public final BaseIterator iterator;

  public TransformIterator(BaseIterator iterator) {
    this.iterator = iterator;
  }

  @Override
  public void reset() throws IOException {
    iterator.reset();
  }

  @Override
  public final boolean isDone() {
    return iterator.isDone();
  }

  @Override
  public boolean hasAllCandidates() {
    return iterator.hasAllCandidates();
  }

  @Override
  public final long currentCandidate() {
    return iterator.currentCandidate();
  }

  @Override
  public boolean hasMatch(ScoringContext context) {
    return iterator.hasMatch(context);
  }

  @Override
  public void syncTo(long identifier) throws IOException {
    iterator.syncTo(identifier);
  }

  @Override
  public void movePast(long identifier) throws IOException {
    iterator.movePast(identifier);
  }

  @Override
  public String getValueString(ScoringContext c) throws IOException {
    return iterator.getValueString(c);
  }

  @Override
  public final long totalEntries() {
    return iterator.totalEntries();
  }
}
