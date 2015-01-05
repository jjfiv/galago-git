package org.lemurproject.galago.utility.btree;

import org.lemurproject.galago.utility.Parameters;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author jfoley.
 */
public interface BTreeWriter extends Closeable {
  /**
   * Returns the current copy of the manifest, which will be stored in
   * the completed index file.  This data is not written until close() is called.
   */
  public abstract Parameters getManifest();

  /**
   * Adds a key-value pair of byte[]s to the index
   *  - when in this form neither the key nor the bytes need to fit into availiable RAM
   *  - this allows multi-gigabyte values to be written to the index
   */
  public abstract void add(IndexElement list) throws IOException;


  /**
   * Closes the index writer
   *  - flushes all buffers and closes the file writers
   */
  @Override
  public abstract void close() throws IOException;
}
