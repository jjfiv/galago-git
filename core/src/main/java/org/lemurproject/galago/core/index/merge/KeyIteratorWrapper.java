/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Uses mapping data to map the keys of a KeyIterator
 * 
 *  --- Assumes the mapping for a key iterator is in ascending order ---
 *      0 -> 10
 *      1 -> (x > 10)
 *
 * @author sjh
 */
public class KeyIteratorWrapper implements Comparable<KeyIteratorWrapper> {
  int id;
  KeyIterator iterator;
  boolean mappingKeys;
  DocumentMappingReader mappingReader;

  public KeyIteratorWrapper(int indexId, KeyIterator iterator, boolean mappingKeys, DocumentMappingReader mappingReader){
    this.iterator = iterator;
    this.id = indexId;
    this.mappingKeys = mappingKeys;
    this.mappingReader = mappingReader;
  }

  // Perform the mapping on the current key

  public byte[] getKeyBytes() throws IOException {
    if(mappingKeys){
      return mappingReader.map(id, this.iterator.getKey());
    } else {
      return this.iterator.getKey();
    }
  }

  public int compareTo(KeyIteratorWrapper o) {
    try {
      return Utility.compare(getKeyBytes(), o.getKeyBytes());
    } catch (IOException ex) {
      Logger.getLogger(KeyIteratorWrapper.class.getName()).log(Level.SEVERE, "There is a problem comparing mapped keys.", ex);
      throw new RuntimeException (ex);
    }
  }

  public boolean nextKey() throws IOException{
    return iterator.nextKey();
  }

  public KeyIterator getIterator() {
    return this.iterator;
  }
  
  public int getIndexId(){
    return this.id;
  }
}
