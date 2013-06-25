// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 *
 * @author irmarc, sjh
 */
public interface DataIterator<T> extends BaseIterator {

  public T data(ScoringContext c);
}
