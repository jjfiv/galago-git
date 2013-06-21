// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.source;

import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author jfoley
 */
public interface ExtentSource extends CountSource {
  public ExtentArray extents(int id);
  public NodeStatistics getStatistics();
}
