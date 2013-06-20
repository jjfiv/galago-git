/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class DiskIterator implements BaseIterator {
  protected ScoringContext context;
  
  // each DiskIterator should have some key
  abstract public String getKeyString() throws IOException;
  abstract public byte[] getKeyBytes() throws IOException;
  
  // could add a few extra functions to the Leaf Node iterator here.
  // This will pass up topdocs information if it's available
  @Override
  public void setContext(ScoringContext context) {
    this.context = context;
  }

  public boolean hasMatch(int identifier) {
    return !isDone() && currentCandidate() == identifier;
  }

  @Override
  public ScoringContext getContext() {
    return context;
  }
}
