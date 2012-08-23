/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Comparator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;

/**
 *
 * @author irmarc
 */
public class IteratorLengthComparator implements Comparator<MovableIterator> {

  @Override
  public int compare(MovableIterator t, MovableIterator t1) {
    return (int) (t.totalEntries() - t1.totalEntries());
  }
}
