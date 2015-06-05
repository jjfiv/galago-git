// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.btree.format;

import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.btree.disk.DiskBTreeWriter;
import org.lemurproject.galago.utility.btree.IndexElement;
import org.lemurproject.galago.utility.debug.Counter;
import org.lemurproject.galago.utility.debug.NullCounter;

import java.io.*;

/**
 * This class writes index files, which are used for most Galago indexes.
 * 
 * An index is a mapping between a key and a value, much like a TreeMap.  The keys are
 * sorted to allow iteration over the whole file.  Keys are stored using prefix
 * compression to save space.  The structure is designed for fast random access on disk.
 * 
 * For indexes, we assume that the data in each value is already compressed, so DiskBTreeWriter
 * does no additional compression.
 *
 * @author trevor, sjh, irmarc, jfoley
 */
public class TupleflowDiskBTreeWriter extends TupleflowBTreeWriter {

  public final DiskBTreeWriter inner;
  Counter recordsWritten = NullCounter.instance;
  Counter blocksWritten = NullCounter.instance;

  /**
   * Creates a new create of DiskBTreeWriter
   */
  public TupleflowDiskBTreeWriter(String outputFilename, Parameters parameters)
          throws IOException {
    this.inner = new DiskBTreeWriter(outputFilename, parameters);
  }

  public TupleflowDiskBTreeWriter(String outputFilename) throws IOException {
    this(outputFilename, Parameters.create());
  }

  public TupleflowDiskBTreeWriter(TupleFlowParameters parameters) throws IOException {
    this(parameters.getJSON().getString("filename"), parameters.getJSON());
    blocksWritten = parameters.getCounter("Blocks Written");
    recordsWritten = parameters.getCounter("Records Written");
  }

  /**
   * Returns the current copy of the manifest, which will be stored in
   * the completed index file.  This data is not written until close() is called.
   */
  @Override
  public Parameters getManifest() {
    return inner.getManifest();
  }

  /**
   * Keys must be in ascending order
   */
  @Override
  public void add(IndexElement list) throws IOException {
    inner.add(list);
    recordsWritten.increment();
  }

  @Override
  public void close() throws IOException {
    System.err.println("calling close!");
    inner.close();
  }
}
