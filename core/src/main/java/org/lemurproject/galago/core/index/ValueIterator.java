// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;

/**
 *
 * @author irmarc, sjh
 */
public interface ValueIterator extends BoundedIterator, Comparable<ValueIterator> {

  int currentCandidate();

  boolean hasMatch(int identifier);

  boolean next() throws IOException;

  boolean moveTo(int identifier) throws IOException;

  void movePast(int identifier) throws IOException;

  String getEntry() throws IOException;
}
