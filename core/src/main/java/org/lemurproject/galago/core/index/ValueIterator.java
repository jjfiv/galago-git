/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;

/**
 *
 * @author sjh
 */
public abstract class ValueIterator implements MovableIterator {
  
  // each ValueIterator should have some key
  abstract public String getKeyString()  throws IOException ;
  abstract public byte[] getKeyBytes()  throws IOException ;

  // could add a few extra functions to the Leaf Node iterator here.
}
