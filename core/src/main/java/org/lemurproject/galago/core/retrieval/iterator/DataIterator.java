// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

/**
 *
 * @author irmarc
 */
public interface DataIterator<T> extends StructuredIterator {
  public T getData();
}
