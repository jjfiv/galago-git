package org.lemurproject.galago.utility.btree;

import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.buffer.DataStream;

import java.io.IOException;

/**
* @author jfoley.
*/
public abstract class BTreeIterator implements Comparable<BTreeIterator> {

  public BTreeReader reader;

  public BTreeIterator(BTreeReader parent) {
    reader = parent;
  }

  /**
   * Returns the current key.
   */
  public abstract byte[] getKey();

  /*
   * find the provided key + move iterator to it
   */
  public abstract void find(byte[] key) throws IOException;

  /*
   * Skip iterator to the provided key - key must be greater than or equal to
   * the current key.
   */
  public abstract void skipTo(byte[] key) throws IOException;

  /**
   * Advances to the next key in the index.
   */
  public abstract boolean nextKey() throws IOException;

  /**
   * Returns true if no more keys remain to be read.
   */
  public abstract boolean isDone();

  /**
   * Returns the length of the value, in bytes.
   */
  public abstract long getValueLength() throws IOException;

  /**
   * Returns the value as a buffered stream.
   */
  public abstract DataStream getValueStream() throws IOException;

  /**
   * Returns a data stream for a subset of the value stream
   */
  public abstract DataStream getSubValueStream(long offset, long length) throws IOException;

  /**
   * Returns the byte offset of the beginning of the current value - note that
   * the corresponding file is returned by
   */
  public abstract long getValueStart() throws IOException;

  /**
   * Returns the byte offset of the end of the current value, relative to the
   * start of the whole inverted file.
   */
  public abstract long getValueEnd() throws IOException;

  /**
   * Returns the value as a string.
   */
  public byte[] getValueBytes() throws IOException {
    DataStream stream = getValueStream();
    assert stream.length() < Integer.MAX_VALUE;
    byte[] data = new byte[(int) stream.length()];
    stream.readFully(data);
    return data;
  }

  /**
   * Returns the value as a string.
   */
  public String getValueString() throws IOException {
    byte[] data = getValueBytes();
    return ByteUtil.toString(data);
  }

  /**
   * Comparator - allows iterators to be read in parallel efficiently
   */
  @Override
  public int compareTo(BTreeIterator i) {
    return CmpUtil.compare(this.getKey(), i.getKey());
  }
}
