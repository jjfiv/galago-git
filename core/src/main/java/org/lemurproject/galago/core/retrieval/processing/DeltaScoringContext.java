// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.ArrayList;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;

/**
 * The scoring context needed for delta-function style processing.
 * 
 * @author irmarc
 */
public class DeltaScoringContext extends ScoringContext {


  public DeltaScoringContext() {
    scorers = new ArrayList<DeltaScoringIterator>();
    quorumIndex = 0;
  }
  public ArrayList<DeltaScoringIterator> scorers;
  public double[] startingPotentials;
  public double[] potentials;
  public double runningScore;
  public double startingPotential;
  public int quorumIndex;
  public double minCandidateScore;
  public boolean stillScoring;
}
