// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.btree.simple;

import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.core.btree.format.DiskBTreeWriter;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Processor;

import java.io.IOException;

/**
 * So if you use this class as a Tupleflow processor, you'd better give us KeyValuePairs in sorted order. This is exactly the magic that happens in DiskMapBuilder. This class is also a pretty thin layer around DiskBTreeWriter.
 *
 * @see DiskMapBuilder
 * @see org.lemurproject.galago.core.btree.format.DiskBTreeWriter
 * @author jfoley
 */
@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key"})
public class DiskMapSortedBuilder implements Processor<KeyValuePair> {
	DiskBTreeWriter btree;
  Parameters opts;
  public DiskMapSortedBuilder(String path, Parameters opts) throws IOException {
    // note that it's okay not to have a readerClass
    if(!opts.isString("readerClass")) {
      opts.put("nonIndexPart", true);
    }
    this.btree = new DiskBTreeWriter(path, opts);
    this.opts = opts;
  }
  
  public DiskMapSortedBuilder(String path) throws IOException {
    this(path, Parameters.create());
  }
  
  /**
   * BTree requires keys put to be in ascending order.
   * @param key what you want to be the key in your btree
   * @param value what you want to be the data in your btree
   * @throws java.io.IOException
   * @see DiskBTreeWriter
   */
  public void put(byte[] key, byte[] value) throws IOException {
    btree.add(new DiskMapElement(key, value));
  }

  public void putCustom(IndexElement element) throws IOException {
    btree.add(element);
  }

  @Override
  public void process(KeyValuePair object) throws IOException {
    put(object.key, object.value);
  }

  /** Call this when done adding keys!
   * @throws java.io.IOException
   */
  @Override
  public void close() throws IOException {
    btree.close();
  }
}
