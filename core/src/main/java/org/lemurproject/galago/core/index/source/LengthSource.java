// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.source;

import org.lemurproject.galago.core.index.stats.FieldStatistics;

/**
 *
 * @author jfoley
 */
public interface LengthSource extends DiskSource {
  public int length(long id);
  public FieldStatistics getStatistics();
}
