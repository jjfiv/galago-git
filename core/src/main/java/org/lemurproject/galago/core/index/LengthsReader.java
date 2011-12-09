// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.index;

import java.io.IOException;

/**
 *
 * @author irmarc
 */
public interface LengthsReader extends IndexPartReader {
  public int getLength(int document) throws IOException;
  public Iterator getLengthsIterator() throws IOException;
  public interface Iterator extends ValueIterator {
    public boolean skipToKey(int candidate) throws IOException;
    public int getCurrentLength() throws IOException;
    public int getCurrentIdentifier() throws IOException;
  }
}
