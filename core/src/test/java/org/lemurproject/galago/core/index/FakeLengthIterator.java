/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author marc
 */
public class FakeLengthIterator implements LengthsIterator {

  private int[] ids;
  private int[] lengths;
  private int position;

  public FakeLengthIterator(int[] i, int[] l) {
    ids = i;
    lengths = l;
    position = 0;
  }

  @Override
  public int length(ScoringContext c) {
    if (c.document == ids[position]) {
      return lengths[position];
    } else {
      return 0;
    }
  }

  @Override
  public long currentCandidate() {
    return ids[position];
  }

  @Override
  public boolean hasMatch(ScoringContext context) {
    return (ids[position] == context.document);
  }

  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public void movePast(long identifier) throws IOException {
    syncTo(identifier + 1);
  }

  @Override
  public void syncTo(long identifier) throws IOException {
    while (!isDone() && ids[position] < identifier) {
      position++;
    }
  }

  @Override
  public void reset() throws IOException {
    position = 0;
  }

  @Override
  public boolean isDone() {
    return (position >= ids.length);
  }

  @Override
  public String getValueString(ScoringContext sc) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long totalEntries() {
    return ids.length;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) {
    String type = "length";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = Integer.toString(length(c));
    List<AnnotatedNode> children = Collections.emptyList();

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
