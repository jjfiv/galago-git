// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

/**
 *
 * @author marc, sjh
 */
public interface IndicatorIterator extends BaseIterator {

  public boolean indicator(long identifier);

}
