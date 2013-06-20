// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

/**
 *
 * @author jfoley
 */
public interface ScoreSource {
  public double score(int id);
}
