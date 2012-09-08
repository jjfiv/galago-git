// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import gnu.trove.map.hash.TObjectIntHashMap;

/**
 *
 * @author irmarc
 */
public class FilteredStatisticsScoringContext extends ScoringContext {
  TObjectIntHashMap<String> tfs;
  TObjectIntHashMap<String> dfs;
  long collectionCount = 0;
  long documentCount = 0;
  public FilteredStatisticsScoringContext() {
    super();
    tfs = new TObjectIntHashMap<String>();
    dfs = new TObjectIntHashMap<String>();
  }
}
