// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
import org.lemurproject.galago.tupleflow.DataStream;

/**
 * Each iterator from an index has an extra two methods,
 * getValueString() and nextKey(), that allows the data from
 * the index to be easily printed.  DumpIndex uses this functionality
 * to dump the contents of any Galago index.
 *
 * (2/22/2011, irmarc): Refactored into the index package to indicate this is functionality
 *                      that a disk-based iterator should have.
 *
 * @author trevor, irmarc
 */
public interface KeyIterator extends StructuredIterator, Comparable<KeyIterator> {

  // moves iterator to some particular key
  boolean findKey(byte[] key) throws IOException;

  // moves iterator to a particular key (forward direction only)
  boolean skipToKey(byte[] key) throws IOException;

  boolean nextKey() throws IOException;

  String getKey() throws IOException;

  byte[] getKeyBytes() throws IOException;

  // Access to the key's value. Not all may be implemented
  String getValueString() throws IOException;

  byte[] getValueBytes() throws IOException;

  DataStream getValueStream() throws IOException;

  ValueIterator getValueIterator() throws IOException;
}
