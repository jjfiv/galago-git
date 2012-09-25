// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;

/**
 *
 * @author irmarc
 */
public interface LengthsReader extends IndexPartReader {

  public int getLength(int document) throws IOException;

  public LengthsIterator getLengthsIterator() throws IOException;

  public interface LengthsIterator extends MovableIterator {
    // This function returns the name of the region:
    // e.g. document, field-name, or #inside(field-name field-name)
    public byte[] getRegionBytes();


    public int getCurrentLength();

    public int getCurrentIdentifier();
  }
}
