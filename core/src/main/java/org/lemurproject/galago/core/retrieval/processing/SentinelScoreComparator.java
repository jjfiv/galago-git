/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Comparator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
public class SentinelScoreComparator implements Comparator<Sentinel> {

  @Override
  public int compare(Sentinel t, Sentinel t1) {
    return Utility.compare(t1.score, t.score);
  }
}
