/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import org.lemurproject.galago.core.types.DocumentMappingData;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.IOException;
import java.util.HashMap;

/**
 * Maps a docid key (bytes) to a new docid key (bytes)
 *
 * @author sjh
 */
public class DocumentMappingReader {

  private HashMap<Integer, Long> indexIncrements = null;

  public DocumentMappingReader() {
    // this constructor creates a null mapping reader
    // --> docId is return unchanged.
  }
  
  public DocumentMappingReader(TypeReader<DocumentMappingData> mappingDataStream) throws IOException {
    indexIncrements = new HashMap();
    DocumentMappingData dat;
    while ((dat = mappingDataStream.read()) != null) {
      indexIncrements.put(dat.indexId, dat.docNumIncrement);
    }
  }

  public long map(int indexId, long docId) {
    if(indexIncrements != null){
      return docId + indexIncrements.get(indexId);
    } else {
      return docId;
    }
  }

  public byte[] map(int indexId, byte[] keyBytes) {
    // TODO stop casting document to int
    return Utility.fromLong(this.map(indexId, Utility.toLong(keyBytes)));
  }
}
