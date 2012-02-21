// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;
import java.lang.reflect.Array;
import java.util.PriorityQueue;

/**
 * An interface that defines the contract for processing a query.
 * There's one method : execute, which takes a fully annotated query
 * tree, and somehow produces a result list.
 *
 *
 * @author irmarc
 */
public abstract class ProcessingModel {
  public abstract ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception;
  public abstract void defineWorkingSet(int[] docs);
    
  public static <T extends ScoredDocument> T[] toReversedArray(PriorityQueue<T> queue) {
    if (queue.size() == 0) {
      return null;
    }

    T[] items = (T[]) Array.newInstance(queue.peek().getClass(), queue.size());
    for (int i = queue.size() - 1; queue.isEmpty() == false; i--) {
      items[i] = queue.poll();
    }
    return items;
  }

}
