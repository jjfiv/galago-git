// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author marc
 */
public class BinaryCountIterator extends TransformIterator implements MovableCountIterator {

  IndicatorIterator indicator;
  
  public BinaryCountIterator(NodeParameters p, MovableIndicatorIterator i) {
    super(i);
    indicator = i;
  }

  @Override
  public int count() {
    return (indicator.indicator(this.currentCandidate()))? 1 : 0 ;
  }

  @Override
  public int maximumCount() {
    return 1;
  }
}
