// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.source;

/**
 *
 * @author jfoley
 */
public interface LengthsSource extends DiskSource {
  public long length(int id);
}
