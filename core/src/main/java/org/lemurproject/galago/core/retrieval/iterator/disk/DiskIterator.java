/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator.disk;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 * Old ValueIterator class (now deprecated)
 *
 * @author sjh
 */
@Deprecated
public abstract class DiskIterator implements BaseIterator {
  
  @Override
  public boolean hasMatch(long identifier) {
    return !isDone() && currentCandidate() == identifier;
  }

  /**
   * For debugging only: getKey as string (it is a byte[]), you don't know the encoding.
   * @return string of byte[] key
   * @throws IOException 
   * @see Utility.toString
   */
  abstract public String getKeyString() throws IOException;
}
