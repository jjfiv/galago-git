// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;

/**
 *
 * @author sjh
 */
public interface NamesReverseReader extends IndexPartReader {

  public long getDocumentIdentifier(String document) throws IOException;
}
