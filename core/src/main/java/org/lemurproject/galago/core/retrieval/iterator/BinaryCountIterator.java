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

  public int count() {
    return iterator.hasMatch(iterator.currentCandidate()) ? 1 : 0;
  }

  public void reset() throws IOException {
    iterator.reset();
  }

  public boolean isDone() {
    return iterator.isDone();
  }

  public int currentCandidate() {
    return iterator.currentCandidate();
  }

  public boolean hasMatch(int identifier) {
    return iterator.hasMatch(identifier);
  }

  public boolean next() throws IOException {
    return iterator.next();
  }

  public boolean moveTo(int identifier) throws IOException {
    return iterator.moveTo(identifier);
  }

  public void movePast(int identifier) throws IOException {
    iterator.movePast(identifier);
  }

  public String getEntry() throws IOException {
    return iterator.getEntry();
  }

  public long totalEntries() {
    return iterator.totalEntries();
  }

  public int compareTo(ValueIterator t) {
    return iterator.compareTo(t);
  }
}
