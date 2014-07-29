// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;

/**
 * Base class for any data structures that map a key value to a list of data,
 * where one cannot assume the list can be held in memory
 *
 *
 * @author irmarc
 */
public abstract class KeyListReader extends KeyValueReader {

  public KeyListReader(String filename) throws IOException {
    super(filename);
  }

  public KeyListReader(BTreeReader r) {
    super(r);
  }
}
