// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.source;

/**
 *
 * @author jfoley
 */
public interface CountSource extends DiskSource {
  public long count(int id);
}
