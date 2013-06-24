// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;

/**
 *
 * @author irmarc
 */
public interface LengthsReader extends IndexPartReader {

  public int getLength(long document) throws IOException;

  public LengthsIterator getLengthsIterator() throws IOException;
}
