// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.index.GenericIndexReader;
import org.lemurproject.galago.core.index.disk.IndexReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.parse.Document;

/**
 * Interface that allows different corpus formats
 * See CorpusReader / DocumentIndexReader / MemoryCorpus
 *
 * @author sjh
 */
public interface DocumentReader extends IndexPartReader {

  public abstract Document getDocument(int key) throws IOException;

  public interface DocumentIterator extends KeyIterator {

    public abstract Document getDocument() throws IOException;
  }
}
