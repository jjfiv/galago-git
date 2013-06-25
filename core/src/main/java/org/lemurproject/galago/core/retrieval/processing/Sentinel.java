/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
public class Sentinel {

  public double score;
  public DeltaScoringIterator iterator;

  public Sentinel(DeltaScoringIterator it, double s) {
    this.score = s;
    this.iterator = it;
  }

  @Override
  public String toString() {
    return String.format("%s -> %f\n", Utility.shortName(iterator), score);
  }
}