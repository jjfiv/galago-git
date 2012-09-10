// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import gnu.trove.map.hash.TObjectIntHashMap;
import java.util.HashMap;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
import org.lemurproject.galago.core.retrieval.query.Node;

/**
 *
 * @author irmarc
 */
public class FilteredStatisticsScoringContext extends ScoringContext
        implements ActiveContext {

  public TObjectIntHashMap<String> tfs;
  public TObjectIntHashMap<String> dfs;
  public long collectionLength = 0;
  public long documentCount = 0;
  HashMap<String, CountIterator> trackedIterators;

  public FilteredStatisticsScoringContext() {
    super();
    tfs = new TObjectIntHashMap<String>();
    dfs = new TObjectIntHashMap<String>();
    trackedIterators = new HashMap<String, CountIterator>();
  }

  @Override
  public void checkIterator(Node node, StructuredIterator iterator) {
    String operator = node.getOperator();
    if (CountIterator.class.isAssignableFrom(iterator.getClass()) &&
            (operator.equals("counts") || operator.equals("extents"))) {
      trackedIterators.put(node.getDefaultParameter(),
              (CountIterator) iterator);
    }
  }

    @Override
  public String toString() {
	StringBuilder sb = new StringBuilder();
	sb.append("collLength=").append(collectionLength);
	sb.append(",docCount=").append(documentCount);
	for (String key : tfs.keySet()) {
	    sb.append(",").append(key).append("_tf=")
		.append(tfs.get(key));
	}
  }
}
