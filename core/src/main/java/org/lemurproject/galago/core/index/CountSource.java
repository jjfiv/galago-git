// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

/**
 *
 * @author jfoley
 */
public interface CountSource extends DataSource {
  public long count(int id);
}
