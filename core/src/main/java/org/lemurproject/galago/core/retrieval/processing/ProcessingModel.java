// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.core.util.FixedSizeMinHeap;

/**
 * An interface that defines the contract for processing a query. There's one
 * method : execute, which takes a fully annotated query tree, and somehow
 * produces a result list.
 *
 *
 * @author irmarc
 */
public abstract class ProcessingModel {

  public abstract ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception;

  public static <T extends ScoredDocument> T[] toReversedArray(PriorityQueue<T> queue) {
    if (queue.size() == 0) {
      return null;
    }

    T[] items = (T[]) Array.newInstance(queue.peek().getClass(), queue.size());
    for (int i = queue.size() - 1; queue.isEmpty() == false; i--) {
      items[i] = queue.poll();

      // set rank attributes here
      items[i].rank = i + 1;
    }
    return items;
  }

  public static <T extends ScoredDocument> T[] toReversedArray(FixedSizeMinHeap<T> queue) {
    if (queue.size() == 0) {
      return null;
    }

    T[] items = queue.getSortedArray();
    int r = 1;
    for (T i : items) {
      i.rank = r;
      r++;
    }

    return items;
  }

  public static ProcessingModel instance(LocalRetrieval r, Node root, Parameters p)
          throws Exception {
    if (p.containsKey("processingModel")) {
      String modelName = p.getString("processingModel");
      Class clazz = Class.forName(modelName);
      Constructor<ProcessingModel> cons = clazz.getConstructor(LocalRetrieval.class);
      return cons.newInstance(r);
    }

    if (p.containsKey("working")) {
      if (p.get("extentQuery", false)) {
        return new WorkingSetExtentModel(r);
      } else if (p.get("passageQuery", false)) {
        return new WorkingSetPassageModel(r);
      } else {
        return new WorkingSetDocumentModel(r);
      }
    }

    QueryType qt = r.getQueryType(root);
    if (qt == QueryType.BOOLEAN) {
      return new SetModel(r);
    } else if (qt == QueryType.RANKED) {
      if (p.containsKey("processingModel")) {
        String modelName = p.getString("processingModel");
        if (modelName.equals("hybrid")) {
          String shortModel = p.getString("shortModel");
          String longModel = p.getString("longModel");
          int nt = (int) p.getLong("numberOfTerms");
          int threshold = (int) p.getLong("modelSwitchLimit");
          if (nt == 1) {
            return new RankedDocumentModel(r);
          } else if (nt < threshold) {
            Class clazz = Class.forName(shortModel);
            Constructor<ProcessingModel> cons = clazz.getConstructor(LocalRetrieval.class);
            return cons.newInstance(r);
          } else {
            Class clazz = Class.forName(longModel);
            Constructor<ProcessingModel> cons = clazz.getConstructor(LocalRetrieval.class);
            return cons.newInstance(r);
          }
        } else {
          Class clazz = Class.forName(modelName);
          Constructor<ProcessingModel> cons = clazz.getConstructor(LocalRetrieval.class);
          return cons.newInstance(r);
        }
      }

      if (p.get("passageQuery", false)) {
        return new RankedPassageModel(r);
      } else {
        if (p.get("deltaReady", false)) {
          return new MaxScoreDocumentModel(r);
        } else {
          return new RankedDocumentModel(r);
        }
      }
    }
    throw new RuntimeException(String.format("Unable to determine processing model for %s",
            root.toString()));
  }
}
