// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.utility.CmpUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author trevor
 * @author irmarc
 */
public class FakeExtentIterator implements ExtentIterator, CountIterator {

  private int[][] data;
  private int index;

  public FakeExtentIterator(int[][] data) {
    this.data = data;
    this.index = 0;
  }

  @Override
  public boolean isDone() {
    return index >= data.length;
  }

  @Override
  public long currentCandidate() {
    if (index < data.length) {
      return data[index][0];
    } else {
      return Long.MAX_VALUE;
    }
  }

  @Override
  public int count(ScoringContext c) {
    if (c.document == currentCandidate()) {
      return data[index].length - 1;
    } else {
      return 0;
    }
  }

  @Override
  public void reset() throws IOException {
    index = 0;
  }

  @Override
  public ExtentArray data(ScoringContext c) {
    return extents(c);
  }

  @Override
  public ExtentArray extents(ScoringContext c) {
    if (c.document == currentCandidate()) {
      ExtentArray array = new ExtentArray();
      int[] datum = data[index];
      array.setDocument(datum[0]);
      for (int i = 1; i < datum.length; i++) {
        array.add(datum[i]);
      }

      return array;
    } else {
      return new ExtentArray();
    }
  }

  @Override
  public boolean hasMatch(ScoringContext context) {
    if (isDone()) {
      return false;
    } else {
      return (currentCandidate() == context.document);
    }
  }

  @Override
  public void syncTo(long identifier) throws IOException {
    while (!isDone() && currentCandidate() < identifier) {
      index++;
    }
  }

  @Override
  public void movePast(long identifier) throws IOException {
    syncTo(identifier + 1);
  }

  @Override
  public long totalEntries() {
    return data.length;
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
    return CmpUtil.compare(currentCandidate(), other.currentCandidate());
  }

  @Override
  public String getValueString(ScoringContext sc) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) {
    String type = "extent";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = extents(c).toString();
    List<AnnotatedNode> children = Collections.emptyList();

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public boolean indicator(ScoringContext c) {
    return count(c) > 0;
  }
}
