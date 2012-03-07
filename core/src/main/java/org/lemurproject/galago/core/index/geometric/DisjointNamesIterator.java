/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.Collection;
import org.lemurproject.galago.core.index.NamesReader;

/**
 *
 * @author sjh
 */
public class DisjointNamesIterator extends DisjointIndexesIterator implements NamesReader.Iterator {

  public DisjointNamesIterator(Collection<NamesReader.Iterator> iterators) {
    super((Collection) iterators);
  }

  @Override
  public String getCurrentName() throws IOException {
    if (head != null) {
      return ((NamesReader.Iterator) this.head).getCurrentName();
    } else {
      throw new IOException("Names Iterator is done.");
    }
  }

  @Override
  public int getCurrentIdentifier() throws IOException {
    if (head != null) {
      return ((NamesReader.Iterator) this.head).getCurrentIdentifier();
    } else {
      throw new IOException("Names Iterator is done.");
    }
  }
}
