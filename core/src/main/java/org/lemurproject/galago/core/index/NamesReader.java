// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.index;

import java.io.IOException;

/**
 *
 * @author irmarc
 */
public interface NamesReader extends IndexPartReader {
  public String getDocumentName(int document) throws IOException;
  public Iterator getNamesIterator() throws IOException;
  public interface Iterator extends ValueIterator {
    public boolean skipToKey(int candidate) throws IOException;
    public String getCurrentName() throws IOException;
    public int getCurrentIdentifier() throws IOException;
  }
}
