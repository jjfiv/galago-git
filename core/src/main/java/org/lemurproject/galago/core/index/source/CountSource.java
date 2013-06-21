// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.source;

import org.lemurproject.galago.core.index.stats.NodeStatistics;

/**
 *
 * @author jfoley
 */
public interface CountSource extends DiskSource {
  public long count(int id);
  public NodeStatistics getStatistics();
}
