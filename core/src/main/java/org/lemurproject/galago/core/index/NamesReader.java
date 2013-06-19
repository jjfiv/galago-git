// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;

/**
 *
 * @author irmarc
 */
public interface NamesReader extends IndexPartReader {

  public String getDocumentName(int document) throws IOException;

  public int getDocumentIdentifier(String document) throws IOException;

  public NamesIterator getNamesIterator() throws IOException;

  public interface NamesIterator extends BaseIterator {

    public String getCurrentName() throws IOException;

    public int getCurrentIdentifier() throws IOException;
  }
}
