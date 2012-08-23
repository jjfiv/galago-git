/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Comparator;

/**
 *
 * @author irmarc
 */
public class SentinelLengthComparator implements Comparator<Sentinel> {

  @Override
  public int compare(Sentinel t, Sentinel t1) {
    return (int) (t.iterator.totalEntries() - t1.iterator.totalEntries());
  }
}
