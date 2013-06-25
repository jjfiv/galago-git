/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.iterator.disk;

import java.io.IOException;
import org.lemurproject.galago.core.index.source.DiskSource;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * This is the base abstract implementation of an Iterator that
 *  wraps a DiskSource.
 * 
 * @author jfoley, sjh
 */
public abstract class SourceIterator extends DiskIterator {

  protected DiskSource source = null;

  public SourceIterator(DiskSource src) {
    source = src;
  }

  // sjh: access to the underlying source is useful occasionally //
  public DiskSource getSource() {
    return source;
  }

  @Override
  public String getKeyString() {
    return source.key();
  }

  @Override
  public void reset() throws IOException {
    source.reset();
  }

  @Override
  public boolean isDone() {
    return source.isDone();
  }

  @Override
  public long currentCandidate() {
    return source.currentCandidate();
  }

  @Override
  public void movePast(long identifier) throws IOException {
    source.movePast(identifier);
  }

  @Override
  public void syncTo(long identifier) throws IOException {
    source.syncTo(identifier);
  }

  @Override
  public boolean hasMatch(long identifier) {
    return source.hasMatch(identifier);
  }

  @Override
  public boolean hasAllCandidates() {
    return source.hasAllCandidates();
  }

  @Override
  public long totalEntries() {
    return source.totalEntries();
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

  // This is not implemented here, because it needs to be customized for each SourceIterator
  @Override
  public abstract String getValueString(ScoringContext c) throws IOException;

  // This is not implemented here, because it needs to be customized for each SourceIterator
  @Override
  public abstract AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException;
}
