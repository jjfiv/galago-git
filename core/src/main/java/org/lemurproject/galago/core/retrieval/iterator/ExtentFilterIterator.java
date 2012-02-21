// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * This class acts as a filtering node over extent nodes. 
 *
 * @author irmarc
 */
public abstract class ExtentFilterIterator implements ExtentValueIterator, CountValueIterator {

  protected ExtentValueIterator extentIterator;

  public ExtentFilterIterator(NodeParameters parameters, ExtentValueIterator extentIterator) {
    this.extentIterator = extentIterator;
  }

  @Override
  public int count() {
    return extents().size();
  }
  
  @Override
  public int maximumCount() {
    return Integer.MAX_VALUE;
  }
  

  @Override
  public ExtentArray getData() {
    return extents();
  }

  @Override
  public void reset() throws IOException {
    extentIterator.reset();
  }

  @Override
  public boolean isDone() {
    return extentIterator.isDone();
  }

  @Override
  public int currentCandidate() {
    return extentIterator.currentCandidate();
  }

  @Override
  public boolean hasMatch(int identifier) {
    return (extentIterator.hasMatch(identifier) && count() > 0);
  }

  @Override
  public boolean next() throws IOException {
    return extentIterator.next();
  }

  @Override
  public boolean moveTo(int identifier) throws IOException {
    return extentIterator.moveTo(identifier);
  }

  @Override
  public void movePast(int identifier) throws IOException {
    extentIterator.movePast(identifier);
  }

  @Override
  public String getEntry() throws IOException {
    return extentIterator.getEntry();
  }

  @Override
  public long totalEntries() {
    return extentIterator.totalEntries();
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
}
