// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Implements the #all indicator operator.
 * @author irmarc
 */
public class UniversalIndicatorIterator extends ConjunctionIterator implements MovableIndicatorIterator {

  public UniversalIndicatorIterator(Parameters globalParams, NodeParameters p, MovableIterator[] children) {
    super(children);
  }

  @Override
  public boolean indicator(int identifier) {
    for(MovableIterator i : this.iterators){
      if(!i.atCandidate(identifier)){
        return false;
      }
    }
    return true;
  }

  @Override
  public String getEntry() throws IOException {
    return this.currentCandidate() + " " + this.indicator(this.currentCandidate());
  }
}
