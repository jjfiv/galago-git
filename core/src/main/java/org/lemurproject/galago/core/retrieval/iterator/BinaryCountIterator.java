// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author marc
 */
public class BinaryCountIterator implements CountValueIterator {

  AbstractIndicator iterator;

  public BinaryCountIterator(NodeParameters p, AbstractIndicator i) {
    iterator = i;
  }

  @Override
  public int count() {
    return iterator.atCandidate(iterator.currentCandidate()) ? 1 : 0;
  }

  @Override
  public int maximumCount() {
    return 1;
  }

  @Override
  public void reset() throws IOException {
    iterator.reset();
  }

  @Override
  public boolean isDone() {
    return iterator.isDone();
  }

  @Override
  public int currentCandidate() {
    return iterator.currentCandidate();
  }

  @Override
  public boolean atCandidate(int identifier) {
    return iterator.atCandidate(identifier);
  }

  @Override
  public boolean next() throws IOException {
    return iterator.next();
  }

  @Override
  public boolean moveTo(int identifier) throws IOException {
    return iterator.moveTo(identifier);
  }

  @Override
  public void movePast(int identifier) throws IOException {
    iterator.movePast(identifier);
  }

  @Override
  public String getEntry() throws IOException {
    return iterator.getEntry();
  }

  @Override
  public long totalEntries() {
    return iterator.totalEntries();
  }

  @Override
  public int compareTo(ValueIterator t) {
    return iterator.compareTo(t);
  }
}
