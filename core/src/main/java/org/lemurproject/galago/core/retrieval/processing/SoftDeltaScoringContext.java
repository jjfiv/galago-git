// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.lemurproject.galago.core.scoring.Estimator;

/**
 * Now also tracks minimum and maximum contributions from the soft scorers
 * So the actual score range is (runningScore + min) to (runningScore + max).
 * 
 * @author irmarc
 */
public class SoftDeltaScoringContext extends DeltaScoringContext {
  public double min;
  public double max;
  public int estimatorIndex;
  public TObjectIntHashMap<Estimator> hi_accumulators;
  public TObjectIntHashMap<Estimator> lo_accumulators;  
  public boolean updating;
  public short[] counts;
  
  public SoftDeltaScoringContext() {
    super();
    hi_accumulators = new TObjectIntHashMap<Estimator>();
    lo_accumulators = new TObjectIntHashMap<Estimator>();
    updating = true;
  }
}
