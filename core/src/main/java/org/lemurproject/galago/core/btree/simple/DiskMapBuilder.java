// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.btree.simple;

import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Sorter;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.utility.Parameters;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author jfoley.
 */
public class DiskMapBuilder implements Closeable {
  public DiskMapSortedBuilder endPoint;
  public Sorter<KeyValuePair> sorter;

  public DiskMapBuilder(String path, Parameters opts) throws IOException {
    this.endPoint = new DiskMapSortedBuilder(path, opts);
    this.sorter = new Sorter<>(new KeyValuePair.KeyOrder());
    try {
      this.sorter.setProcessor(endPoint);
    } catch (IncompatibleProcessorException e) {
      // I love comments that say this, but this should never happen,
      // because I know that the processors are compatible because
      // I picked them to both operate on KeyValuePair objects
      throw new RuntimeException(e);
    }
  }

  public DiskMapBuilder(String path) throws IOException {
    this(path, Parameters.create());
  }

  public void put(byte[] key, byte[] value) throws IOException {
    sorter.process(new KeyValuePair(key, value));
  }

  /** Call this when done adding keys!
   * @throws java.io.IOException
   */
  public void close() throws IOException {
    sorter.close(); // probably takes a while to flush everything...
    // sorter will close endpoint for us!
    // endPoint.close(); // now finish writing this guy
  }
}
