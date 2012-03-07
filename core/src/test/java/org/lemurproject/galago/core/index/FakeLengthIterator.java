/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;

/**
 *
 * @author marc
 */
public class FakeLengthIterator implements LengthsReader.Iterator {
  int[] ids;
  int[] lengths;
  int position;
  
  public FakeLengthIterator(int[] i, int[] l) {
    ids = i;
    lengths = l;
    position = 0;
  }
  
  
  @Override
  public int getCurrentLength() {
    return lengths[position];
  }

  @Override
  public int getCurrentIdentifier() {
    return ids[position];
  }

  @Override
  public int currentCandidate() {
    return ids[position];
  }

  @Override
  public boolean atCandidate(int identifier) {
    return (ids[position] == identifier);
  }

  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public void next() throws IOException {
    position = Math.min(position+1, ids.length);
  }

  @Override
  public void movePast(int identifier) throws IOException {
    moveTo(identifier+1);
  }

  @Override
  public void moveTo(int identifier) throws IOException {
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
  public String getEntry() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long totalEntries() {
    return ids.length;
  }

  @Override
  public int compareTo(MovableIterator t) {
    throw new UnsupportedOperationException("Not supported yet.");
  } 
}
