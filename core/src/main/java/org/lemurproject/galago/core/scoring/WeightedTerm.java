// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.scoring;

/**
 * A generic interface for weighted 'terms', although it may also apply to
 * phrases. ExpansionModel implementations each have their own internal implementation
 * of this interface.
 * 
 * @author irmarc
 */
public interface WeightedTerm extends Comparable<WeightedTerm> {
    public String getTerm();
    public double getWeight();
    public int compareTo(WeightedTerm other);
}