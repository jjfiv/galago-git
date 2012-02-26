// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;

/**
 *
 * @author irmarc
 */
public interface NamesReader extends IndexPartReader {
  public String getDocumentName(int document) throws IOException;
  public Iterator getNamesIterator() throws IOException;
  public int getDocumentIdentifier(String document) throws IOException;
  public interface Iterator extends MovableIterator {
    public String getCurrentName() throws IOException;
    public int getCurrentIdentifier() throws IOException;
  }
}
