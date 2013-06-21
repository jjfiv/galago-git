// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.source;

/**
 *
 * @author jfoley
 */
public interface ScoreSource extends DiskSource {
  public double score(long id);
  public double maxScore();
  public double minScore();
}
