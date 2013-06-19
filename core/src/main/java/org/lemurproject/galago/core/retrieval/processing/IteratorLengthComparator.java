/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Comparator;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;

/**
 *
 * @author irmarc
 */
public class IteratorLengthComparator implements Comparator<BaseIterator> {

  @Override
  public int compare(BaseIterator t, BaseIterator t1) {
    return (int) (t.totalEntries() - t1.totalEntries());
  }
}
