// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.lemurproject.galago.core.retrieval.iterator.ModifiableIterator;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Base class for any data structures that map a key value to a list of data,
 * where one cannot assume the list can be held in memory
 *
 *
 * @author irmarc
 */
public abstract class KeyListReader extends KeyValueReader {

  public KeyListReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
  }

  public KeyListReader(BTreeReader r) {
    super(r);
  }
}
