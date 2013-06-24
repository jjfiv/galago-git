// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.IOException;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;

/**
 * Interface that allows different corpus formats See CorpusReader /
 * DocumentIndexReader / MemoryCorpus
 *
 * @author sjh
 */
public interface DocumentReader extends IndexPartReader {

  public abstract Document getDocument(byte[] key, DocumentComponents p) throws IOException;

  public abstract Document getDocument(long key, DocumentComponents p) throws IOException;

  public interface DocumentIterator extends KeyIterator {

    public abstract Document getDocument(DocumentComponents p) throws IOException;
  }
}
