// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author trevor
 * @author irmarc
 */
public class NullExtentIterator implements ExtentValueIterator, MovableCountIterator {

  ExtentArray array = new ExtentArray();

  public NullExtentIterator(){
    
  }
  
  public NullExtentIterator(NodeParameters p){
    // nothing
  }
  
  @Override
  public boolean next() {
    // do nothing
    return false;
  }

  public boolean nextEntry() {
    return false;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public ExtentArray extents() {
    return array;
  }

  @Override
  public int count() {
    return 0;
  }

  @Override
  public int maximumCount() {
    return 0;
  }
  
  @Override
  public void reset() {
    // do nothing
  }

  @Override
  public ExtentArray getData() {
    return array;
  }

  @Override
  public long totalEntries() {
    return 0;
  }

  @Override
  public int currentCandidate() {
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean atCandidate(int id) {
    return false;
  }

  @Override
  public String getEntry() throws IOException {
    return "NULL";
  }

  @Override
  public boolean moveTo(int identifier) throws IOException {
    return false;
  }

  @Override
  public void movePast(int identifier) throws IOException {
  }

  @Override
  public int compareTo(ValueIterator t) {
    return 1;
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }
}
