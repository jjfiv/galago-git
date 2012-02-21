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
public class NullExtentIterator implements ExtentValueIterator, CountValueIterator {

  ExtentArray array = new ExtentArray();

  public NullExtentIterator(){
    
  }
  
  public NullExtentIterator(NodeParameters p){
    // nothing
  }
  
  public boolean next() {
    // do nothing
    return false;
  }

  public boolean nextEntry() {
    return false;
  }

  public boolean isDone() {
    return true;
  }

  public ExtentArray extents() {
    return array;
  }

  public int count() {
    return 0;
  }

  public int maximumCount() {
    return 0;
  }
  
  public void reset() {
    // do nothing
  }

  public ExtentArray getData() {
    return array;
  }

  public long totalEntries() {
    return 0;
  }

  public int currentCandidate() {
    return Integer.MAX_VALUE;
  }

  public boolean hasMatch(int id) {
    return false;
  }

  public String getEntry() throws IOException {
    return "NULL";
  }

  public boolean moveTo(int identifier) throws IOException {
    return false;
  }

  public void movePast(int identifier) throws IOException {
  }

  public int compareTo(ValueIterator t) {
    return 1;
  }
}
