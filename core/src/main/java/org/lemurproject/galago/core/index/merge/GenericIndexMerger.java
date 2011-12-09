// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.merge;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class GenericIndexMerger<T> {

  // document mapping data
  protected DocumentMappingReader mappingReader = null;
  // input readers
  protected PriorityQueue<KeyIteratorWrapper> queue;
  protected HashMap<KeyIteratorWrapper, Integer> partIds;
  // output writer
  protected Processor<T> writer = null;

  public GenericIndexMerger(TupleFlowParameters parameters) throws Exception {
    queue = new PriorityQueue();
    partIds = new HashMap();

    writer = createIndexWriter(parameters);
  }

  public void setDocumentMapping(DocumentMappingReader mappingReader) {
    this.mappingReader = mappingReader;
  }

  // this requires that the mappingReader has been set.
  public void setInputs(HashMap<IndexPartReader, Integer> readers) throws IOException {
    for (IndexPartReader r : readers.keySet()) {
      KeyIterator iterator = r.getIterator();
      if (iterator != null) {
        KeyIteratorWrapper w = new KeyIteratorWrapper(readers.get(r), iterator, 
                (mappingKeys() && mappingReader != null), mappingReader);
        queue.offer(w);
        partIds.put(w, readers.get(r));
      }
    }
  }

  public void performKeyMerge() throws IOException {
    ArrayList<KeyIteratorWrapper> head = new ArrayList();
    while (!queue.isEmpty()) {
      head.clear();
      head.add(queue.poll());
      while ((!queue.isEmpty())
              && (queue.peek().compareTo(head.get(0)) == 0)) {
        head.add(queue.poll());
      }

      byte[] key = head.get(0).getKeyBytes();

      performValueMerge(key, head);

      for (KeyIteratorWrapper i : head) {
        if (i.nextKey()) {
          queue.offer(i);
        }
      }
    }
  }

  public void close() throws IOException {
    writer.close();
  }

  // returns a boolean value that indicates if the index key is to be mapped
  public abstract boolean mappingKeys();
  
  // creates the writer object - needs to be implemented for each merger
  public abstract Processor<T> createIndexWriter(TupleFlowParameters parameters) throws Exception;

  // merges the of values for the current key
  public abstract void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException;
}
