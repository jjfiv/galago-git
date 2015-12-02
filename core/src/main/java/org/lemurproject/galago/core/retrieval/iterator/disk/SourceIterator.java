/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.iterator.disk;

import org.lemurproject.galago.core.index.source.DiskSource;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

import java.io.IOException;

/**
 * This is the base abstract implementation of an Iterator that
 *  wraps a DiskSource.
 * 
 * @author jfoley, sjh
 */
public abstract class SourceIterator implements BaseIterator {

  protected final DiskSource source;

  public SourceIterator(DiskSource src) {
    source = src;
  }

  // sjh: access to the underlying source is useful occasionally //
  public DiskSource getSource() {
    return source;
  }

  public String getKeyString() {
    return source.key();
  }

  @Override
  public final void reset() throws IOException {
    source.reset();
  }

  @Override
  public final boolean isDone() {
    return source.isDone();
  }

  @Override
  public final long currentCandidate() {
    return source.currentCandidate();
  }

  @Override
  public final void movePast(long identifier) throws IOException {
    source.movePast(identifier);
  }

  @Override
  public final void syncTo(long identifier) throws IOException {
    source.syncTo(identifier);
  }

  @Override
  public final boolean hasMatch(ScoringContext context) {
    return source.hasMatch(context.document);
  }

  @Override
  public final boolean hasAllCandidates() {
    return source.hasAllCandidates();
  }

  @Override
  public final long totalEntries() {
    return source.totalEntries();
  }

  // This is not implemented here, because it needs to be customized for each SourceIterator
  @Override
  public abstract String getValueString(ScoringContext c) throws IOException;

  // This is not implemented here, because it needs to be customized for each SourceIterator
  @Override
  public abstract AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException;
}
