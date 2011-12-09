// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.structured;

import gnu.trove.TIntArrayList;

/**
 * Carries an additional working set of documents to score.
 * 
 * @author irmarc
 */
public class WorkingSetContext extends ScoringContext {
  public TIntArrayList workingSet;
}
