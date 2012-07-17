// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.util.ExtentArray;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.iterator.MovableExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

/**
 *
 * @author trevor
 * @author irmarc
 */
public class FakeExtentIterator implements MovableExtentIterator, MovableCountIterator {

  private int[][] data;
  private int index;
  private ScoringContext context;

  public FakeExtentIterator(int[][] data) {
    this.data = data;
    this.index = 0;
  }

  @Override
  public boolean isDone() {
    return index >= data.length;
  }

  @Override
  public int currentCandidate() {
    return data[index][0];
  }

  @Override
  public int count() {
    return data[index].length - 1;
  }

  @Override
  public void reset() throws IOException {
    index = 0;
  }

  @Override
  public ExtentArray getData() {
    return extents();
  }

  @Override
  public ExtentArray extents() {
    ExtentArray array = new ExtentArray();
    int[] datum = data[index];
    array.setDocument(datum[0]);
    for (int i = 1; i < datum.length; i++) {
      array.add(datum[i]);
    }

    return array;
  }

  @Override
  public boolean hasMatch(int identifier) {
    if (isDone()) {
      return false;
    } else {
      return (currentCandidate() == identifier);
    }
  }

  @Override
  public void moveTo(int identifier) throws IOException {
    while (!isDone() && currentCandidate() < identifier) {
      index++;
    }
  }

  @Override
  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  @Override
  public long totalEntries() {
    return data.length;
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
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int maximumCount() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public void setContext(ScoringContext context) {
    this.context = context;
  }

  @Override
  public ScoringContext getContext() {
    return context;
  }

  @Override
  public AnnotatedNode getAnnotatedNode() {
    String type = "count";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = extents().toString();
    List<AnnotatedNode> children = Collections.EMPTY_LIST;

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
