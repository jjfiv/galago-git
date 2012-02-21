// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import gnu.trove.map.hash.TObjectIntHashMap;
import java.util.ArrayList;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;

/**
 * The scoring context needed for delta-function style processing over field models.
 * 
 * @author irmarc
 */
public class FieldDeltaScoringContext extends DeltaScoringContext {


  public FieldDeltaScoringContext() {
    super();
    lengths = new TObjectIntHashMap<String>();
  }
  public TObjectIntHashMap<String> lengths;
}
