// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import gnu.trove.list.array.TIntArrayList;

/**
 * Carries an additional working set of documents to score.
 * 
 * @author irmarc
 */
public class WorkingSetContext extends ScoringContext {
  public TIntArrayList workingSet;
}
