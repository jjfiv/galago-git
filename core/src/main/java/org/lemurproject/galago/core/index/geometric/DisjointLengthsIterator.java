/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.Collection;
import org.lemurproject.galago.core.index.LengthsReader;

/**
 *
 * @author sjh
 */
public class DisjointLengthsIterator extends DisjointIndexesIterator implements LengthsReader.Iterator {

  public DisjointLengthsIterator(Collection<LengthsReader.Iterator> iterators) {
    super((Collection) iterators);
  }

  public int getCurrentLength() {
    if (head != null) {
      return ((LengthsReader.Iterator) this.head).getCurrentLength();
    } else {
      throw new RuntimeException("Lengths Iterator is done.");
    }
  }

  public int getCurrentIdentifier() {
    if (head != null) {
      return ((LengthsReader.Iterator) this.head).getCurrentIdentifier();
    } else {
      throw new RuntimeException("Lengths Iterator is done.");
    }
  }
}
