// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.utility.CmpUtil;

/**
 * Special class to allow iterator over an array of extents
 * 
 * @author trevor
 */
public class ExtentArrayIterator implements Comparable<ExtentArrayIterator> {

  ExtentArray array;
  int index;

  public ExtentArrayIterator(ExtentArray array) {
    this.array = array;
    index = 0;
  }

  public void resetIterator(){
    index = 0;
  }
  
  public int getDocument() {
    // TODO stop casting document to int
    return (int) array.getDocument();
  }

  public int currentBegin() {
    return array.begin(index);
  }

  public int currentEnd() {
    return array.end(index);
  }

  public boolean currentlyContains(ExtentArrayIterator other) {
    return (this.currentBegin() <= other.currentBegin()
            && this.currentEnd() >= other.currentEnd());
  }

  public boolean next() {
    index += 1;
    return index < array.size();
  }

  public boolean isDone() {
    return array.size() <= index;
  }

  @Override
  public int compareTo(ExtentArrayIterator iterator) {
    int result = CmpUtil.compare(array.getDocument(), iterator.array.getDocument());

    if (result != 0) {
      return result;
    }
    return currentBegin() - iterator.currentBegin();
  }
}
