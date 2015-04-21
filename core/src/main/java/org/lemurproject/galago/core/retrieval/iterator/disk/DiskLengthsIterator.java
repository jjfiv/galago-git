// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.disk;

import org.lemurproject.galago.core.index.disk.DiskLengthSource;
import org.lemurproject.galago.core.index.source.LengthSource;
import org.lemurproject.galago.core.index.stats.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * DiskLengthsIterator wraps the disk-based DiskLengthSource
 *
 * @author jfoley
 * @see DiskLengthSource
 */
public class DiskLengthsIterator extends SourceIterator
        implements LengthsIterator, CollectionAggregateIterator {

  LengthSource lengthSrc;

  public DiskLengthsIterator(LengthSource src) throws IOException {
    super(src);
    lengthSrc = src;
  }

  @Override
  public String getValueString(ScoringContext c) throws IOException {
    return currentCandidate() + "," + length(c);
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "lengths";
    String className = this.getClass().getSimpleName();
    String parameters = getKeyString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = Integer.toString(length(c));
    List<AnnotatedNode> children = Collections.emptyList();
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public int length(ScoringContext c) {
    return lengthSrc.length(c.document);
  }

  @Override
  public FieldStatistics getStatistics() {
    return lengthSrc.getStatistics();
  }
}
