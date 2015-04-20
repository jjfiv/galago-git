// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.btree.format.BTreeFactory;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;

/**
 * Superclass for any iterator that provides a direct mapping
 * of keys -> values, and we assume the entire value can be read into
 * memory.
 *
 * Implementations should provide their own methods for manipulating the keys and
 * values.
 * 
 * @author irmarc
 */
public abstract class KeyValueReader implements IndexPartReader {

  public BTreeReader reader;

  public KeyValueReader(String filename) throws IOException {
    reader = BTreeFactory.getBTreeReader(filename);
  }

  public KeyValueReader(BTreeReader r) {
    this.reader = r;
  }

  @Override
  public Parameters getManifest() {
    return reader.getManifest();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  public static abstract class KeyValueIterator implements KeyIterator {

    public BTreeIterator iterator;
    public BTreeReader reader;

    public KeyValueIterator(BTreeReader reader) throws IOException {
      this.reader = reader;
      reset();
    }

    @Override
    public boolean findKey(byte[] key) throws IOException {
      iterator.find(key);
      return !isDone();
    }

    @Override
    public boolean skipToKey(byte[] key) throws IOException {
      iterator.skipTo(key);
      return !isDone();
    }

    @Override
    public boolean nextKey() throws IOException {
      iterator.nextKey();
      return !isDone();
    }

    @Override
    public boolean isDone() {
      return iterator.isDone();
    }

    @Override
    public void reset() throws IOException {
      iterator = reader.getIterator();
    }

    @Override
    public byte[] getKey() {
      return iterator.getKey();
    }

    @Override
    public byte[] getValueBytes() throws IOException{
      return iterator.getValueBytes();
    }
    
    @Override
    public int compareTo(KeyIterator other) {
      try {
        return CmpUtil.compare(getKey(), other.getKey());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
}
