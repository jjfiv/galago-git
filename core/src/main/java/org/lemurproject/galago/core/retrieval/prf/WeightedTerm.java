// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.prf;

import org.lemurproject.galago.utility.lists.Scored;

/**
 * A generic interface for weighted terms, although it may also apply to
 * phrases. ExpansionModel implementations each have their own internal implementation
 * of this interface.
 * 
 * @author irmarc
 */
public abstract class WeightedTerm extends Scored implements Comparable<WeightedTerm> {

  public WeightedTerm(double score) {
    super(score);
  }

  public double getWeight() { return score; }
  public abstract String getTerm();
  public abstract int compareTo(WeightedTerm other);
}