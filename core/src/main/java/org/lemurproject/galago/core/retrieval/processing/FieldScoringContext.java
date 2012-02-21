// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import gnu.trove.map.hash.TObjectIntHashMap;

/**
 * Adds the processing of fields to scoring 
 *
 * @author irmarc
 */
public class FieldScoringContext extends ScoringContext {

  public TObjectIntHashMap<String> lengths;
  
  public FieldScoringContext() {
    super();
    lengths = new TObjectIntHashMap<String>();
  }  
}
