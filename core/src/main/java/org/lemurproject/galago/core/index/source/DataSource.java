// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.source;

import java.io.IOException;

/**
 *
 * @author jfoley
 */
public interface DataSource<T> extends DiskSource {
  T getData(long id) throws IOException;
}
