// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

import java.io.IOException;
import java.util.function.Consumer;

/**
 *
 * @author irmarc, sjh
 */
public interface DataIterator<T> extends BaseIterator {

  T data(ScoringContext c);

  default void forEachData(Consumer<T> onValue) throws IOException {
    forEach((ctx) -> onValue.accept(data(ctx)));
  }
}
