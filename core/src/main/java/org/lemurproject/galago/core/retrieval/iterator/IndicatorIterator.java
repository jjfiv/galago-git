// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.index.ValueIterator;

/**
 *
 * @author marc, sjh
 */
public interface IndicatorIterator extends ValueIterator, StructuredIterator {

  // use hasMatch(document) == true to indicate 'true'
}
