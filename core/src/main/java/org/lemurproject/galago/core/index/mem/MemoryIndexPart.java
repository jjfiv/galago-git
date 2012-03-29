/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.mem;

import java.io.IOException;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;

/**
 *
 * @author sjh
 */
public interface MemoryIndexPart extends IndexPartReader {
  /*
   * flush data to a disk index part
   */
  public void flushToDisk(String path) throws IOException;

  /*
   * extract data from document and add to index
   */
  public void addDocument(Document doc) throws IOException;
  
  /*
   * extract all data from iterator and add to index
   *  - should not expose partial information (oh-to-be-thread-safe)
   *  - key
   */
  public void addIteratorData(byte[] key, MovableIterator iterator) throws IOException;
  
  /*
   * return the current number of documents in index part
   */
  public long getDocumentCount();
  
  /*
   * return the current number of terms in index part
   */
  public long getCollectionLength();

  /*
   * return the current number of keys in index
   */
  public long getKeyCount();
}
