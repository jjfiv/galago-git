/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.mem;

import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;

import java.io.IOException;

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
   */
  public void addIteratorData(byte[] key, BaseIterator iterator) throws IOException;

  /*
   * discard all data for a key
   *  - this allows dynamic deletion of cached iterator data
   */
  public void removeIteratorData(byte[] fromString) throws IOException;
  
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

  /*
   * return a cached iterator - specifically used byte CachedRetrieval
   */
  public BaseIterator getIterator(byte[] key) throws IOException;
}
