// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import gnu.trove.map.hash.TObjectIntHashMap;
import java.util.HashMap;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.Node;

/**
 *
 * @author irmarc
 */
public class FilteredStatisticsScoringContext extends ScoringContext
        implements ActiveContext {

  public TObjectIntHashMap<Node> tfs;
  public TObjectIntHashMap<Node> dfs;
  public long collectionLength = 0;
  public long documentCount = 0;
  HashMap<MovableIterator, Node> iteratorsToNodes;
  
  public FilteredStatisticsScoringContext() {
    super();
    tfs = new TObjectIntHashMap<Node>();
    dfs = new TObjectIntHashMap<Node>();
    iteratorsToNodes = new HashMap<MovableIterator, Node>();
  }

  @Override
  public void checkIterator(Node node, MovableIterator iterator) {
    iteratorsToNodes.put(iterator, node);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("collLength=").append(collectionLength);
    sb.append(",docCount=").append(documentCount);
    for (Node key : tfs.keySet()) {
      sb.append(",").append(key.toString()).append("_tf=").append(tfs.get(key));
      sb.append(",").append(key.toString()).append("_df=").append(dfs.get(key));
    }
    return sb.toString();
  }
}
