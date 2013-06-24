// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskDataIterator;

/**
 *
 * @author irmarc, sjh
 */
public interface NamesReader extends IndexPartReader {

  public String getDocumentName(long document) throws IOException;

  public DiskDataIterator<String> getNamesIterator() throws IOException;
}
