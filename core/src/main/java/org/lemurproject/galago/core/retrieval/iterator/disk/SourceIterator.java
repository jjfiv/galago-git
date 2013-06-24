/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.iterator.disk;

import java.io.IOException;
import org.lemurproject.galago.core.index.source.DiskSource;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

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
  public int currentCandidate() {
    return (int) source.currentCandidate();
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
  public boolean hasMatch(int identifier) {
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
    return currentCandidate() - other.currentCandidate();
  }

  // This is not implemented here, because it needs to be customized for each SourceIterator
  @Override
  public abstract String getValueString() throws IOException;

  // This is not implemented here, because it needs to be customized for each SourceIterator
  @Override
  public abstract AnnotatedNode getAnnotatedNode() throws IOException;
  
}
