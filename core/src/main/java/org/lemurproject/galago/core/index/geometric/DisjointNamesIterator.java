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

  public String getCurrentName() throws IOException {
    if (head != null) {
      return ((NamesReader.Iterator) this.head).getCurrentName();
    } else {
      throw new IOException("Names Iterator is done.");
    }
  }

  public int getCurrentIdentifier() throws IOException {
    if (head != null) {
      return ((NamesReader.Iterator) this.head).getCurrentIdentifier();
    } else {
      throw new IOException("Names Iterator is done.");
    }
  }

  public boolean nextKey() throws IOException {
    return moveTo(currentCandidate() + 1);
  }

  public boolean skipToKey(int candidate) throws IOException {
    queue.offer(head);
    while(!queue.isEmpty()){
      head = queue.poll();
      if(((NamesReader.Iterator) head).moveTo(candidate)){
        return true;
      } else if(!head.isDone()){
        return false;
      }
    }
    return false;
  }
}
