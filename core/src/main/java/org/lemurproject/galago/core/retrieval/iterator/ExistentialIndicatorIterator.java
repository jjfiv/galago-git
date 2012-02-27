// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * Implements the #any indicator operator.
 * @author irmarc
 */
public class ExistentialIndicatorIterator extends DisjunctionIterator implements MovableIndicatorIterator {

  public ExistentialIndicatorIterator(NodeParameters p, MovableIterator[] children) {
    super(children);
  }

  @Override
  public boolean indicator(int identifier) {
    for (MovableIterator i : this.iterators) {
      if (!i.isDone() && i.atCandidate(identifier)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getEntry() throws IOException {
    return this.currentCandidate() + " " + this.indicator(this.currentCandidate());
  }
}
