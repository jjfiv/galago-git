/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.mem;

import java.io.IOException;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.parse.Document;

/**
 *
 * @author sjh
 */
public interface MemoryIndexPart extends IndexPartReader {
  
  public void flushToDisk(String path) throws IOException;
  public void addDocument(Document doc) throws IOException;
  public long getDocumentCount();
  public long getCollectionLength();
  public long getVocabCount();
}
