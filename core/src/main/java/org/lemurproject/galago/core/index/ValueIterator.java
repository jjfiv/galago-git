// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;

/**
 *
 * @author irmarc, sjh
 */
public interface ValueIterator extends BoundedIterator, Comparable<ValueIterator> {

  void movePast(int identifier) throws IOException;
  boolean moveTo(int identifier) throws IOException;
  boolean next() throws IOException;

  int currentCandidate();
  boolean atCandidate(int identifier);

  String getEntry() throws IOException;
}
