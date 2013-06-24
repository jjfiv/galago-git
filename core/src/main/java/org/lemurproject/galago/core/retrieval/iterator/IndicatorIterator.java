// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 *
 * @author marc, sjh
 */
public interface IndicatorIterator extends BaseIterator {

  public boolean indicator(ScoringContext c);

}
