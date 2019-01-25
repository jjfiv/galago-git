// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.source;

/**
 *
 * @author jfoley, sjh
 */
public interface DataSource<T> extends DiskSource {

  T data(long id);
}
