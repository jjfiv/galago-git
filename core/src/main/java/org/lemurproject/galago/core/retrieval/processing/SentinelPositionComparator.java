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
public class SentinelPositionComparator implements Comparator<Sentinel> {

  @Override
  public int compare(Sentinel s1, Sentinel s2) {
    if (s2.iterator.isDone()) {
      return -1;
    }
    if (s1.iterator.isDone()) {
      return 1;
    }
    return (s1.iterator.currentCandidate() - s2.iterator.currentCandidate());
  }
}
