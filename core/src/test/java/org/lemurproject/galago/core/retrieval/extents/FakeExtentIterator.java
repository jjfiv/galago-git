// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.util.ExtentArray;
import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.CountValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentValueIterator;

/**
 *
 * @author trevor
 * @author irmarc
 */
public class FakeExtentIterator implements ExtentValueIterator, CountValueIterator {

  int[][] data;
  int index;

  public FakeExtentIterator(int[][] data) {
    this.data = data;
    this.index = 0;
  }

  @Override
  public boolean next() {
    if (index < data.length) {
      index++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean isDone() {
    return index >= data.length;
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
  public boolean atCandidate(int identifier) {
    if (isDone()) return false;
    else return (currentCandidate() == identifier);
  }

  @Override
  public boolean moveTo(int identifier) throws IOException {
    while (!isDone() && currentCandidate() < identifier) {
      index++;
    }
    return atCandidate(identifier);
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

  @Override
  public String getEntry() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int maximumCount() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
