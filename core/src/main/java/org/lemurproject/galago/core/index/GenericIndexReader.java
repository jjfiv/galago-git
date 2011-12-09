// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.index.disk.VocabularyReader;
import org.lemurproject.galago.core.index.disk.IndexReader;
import java.io.IOException;
import org.lemurproject.galago.core.index.corpus.SplitIndexReader;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * <p>This implements the core functionality for all inverted list readers.  It can
 * also be used as a read-only TreeMap for disk-based data structures.  In Galago,
 * it is used both to store index data and to store documents.</p>
 * 
 * <p>An index is a mapping from String to byte[].  If compression is turned on, the
 * value must be small enough that it fits in memory.  If compression is off, values
 * are streamed directly from disk so there is no size restriction.  Indexes support
 * iteration over all keys, or direct lookup of a single key.  The structure is optimized
 * to support fast random lookup on disks.</p>
 * 
 * <p>Data is stored in blocks, typically 32K each.  Each block has a prefix-compressed
 * set of keys at the beginning, followed by a block of value data. </p>
 * 
 * <p>Typically this class is extended by composition instead of inheritance.</p>
 *
 * <p> (11/29/2010, irmarc): After conferral with Sam, going to remove the requirement
 * that keys be Strings. It makes the mapping from other classes/primitives to Strings
 * really restrictive if they always have to be mapped to Strings. Therefore, mapping
 * byte[] keys to the client keyspace is the responsibility of the client of the IndexReader.</p>
 *
 * Comments copied from IndexReader: author trevor, irmarc, sjh
 *
 * @author sjh
 */
public abstract class GenericIndexReader {

  public abstract class Iterator implements Comparable<Iterator> {

    /**
     * Returns the current key.
     */
    public abstract byte[] getKey();

    /*
     * find the provided key + move iterator to it
     */
    public abstract void find(byte[] key) throws IOException;

    /*
     * Skip iterator to the provided key
     *  - key must be greater than or equal to the current key.
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
    public abstract DataStream getSubValueStream(long offset, long length) throws IOException ;


      /**
       *  Returns the byte offset
       *  of the beginning of the current value
       *   - note that the corresponding file is returned by
       */
    

    public abstract long getValueStart() throws IOException;

    /**
     * Returns the byte offset
     * of the end of the current value,
     * relative to the start of the whole inverted file.
     */
    public abstract long getValueEnd() throws IOException;

    //**********************//
    // Implemented Functions
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
      return Utility.toString(data);
    }

    /**
     * Comparator - allows iterators to be read in parallel efficiently
     */
    public int compareTo(GenericIndexReader.Iterator i) {
      return Utility.compare(this.getKey(), i.getKey());
    }
  }

  // Abstract functions
  /**
   * Returns a Parameters object that contains metadata about
   * the contents of the index.  This is the place to store important
   * data about the index contents, like what stemmer was used or the
   * total number of terms in the collection.
   */
  public abstract Parameters getManifest();

  /**
   * Returns the vocabulary structure for this IndexReader.
   *  - Note that the vocabulary contains only
   *    the first key in each block.
   */
  public abstract VocabularyReader getVocabulary();

  /**
   * Returns an iterator pointing to the very first key in the index.
   * This is typically used for iterating through the entire index,
   * which might be useful for testing and debugging tools, but probably
   * not for traditional document retrieval.
   */
  public abstract Iterator getIterator() throws IOException;

  /**
   * Returns an iterator pointing at a specific key.  Returns
   * null if the key is not found in the index.
   */
  public abstract Iterator getIterator(byte[] key) throws IOException;

  /**
   * Closes all files associated with the IndexReader.
   */
  public abstract void close() throws IOException;

  // Implemented functions
  /**
   * Returns the value stored in the index associated with this key.
   */
  public String getValueString(byte[] key) throws IOException {
    Iterator iter = getIterator(key);

    if (iter == null) {
      return null;
    }

    return iter.getValueString();
  }

  /**
   * Returns the value stored in the index associated with this key.
   */
  public byte[] getValueBytes(byte[] key) throws IOException {
    Iterator iter = getIterator(key);

    if (iter == null) {
      return null;
    }
    return iter.getValueBytes();
  }

  /**
   * Gets the value stored in the index associated with this key.
   *
   * @param key
   * @return The index value for this key, or null if there is no such value.
   * @throws java.io.IOException
   */
  public DataStream getValueStream(byte[] key) throws IOException {
    Iterator iter = getIterator(key);

    if (iter == null) {
      return null;
    }
    return iter.getValueStream();
  }

  /*
   * Static function to open an index file or folder
   */
  public static GenericIndexReader getIndexReader(String pathname) throws IOException {
    if (SplitIndexReader.isParallelIndex(pathname)) {
      return new SplitIndexReader(pathname);
    } else if (IndexReader.isIndexFile(pathname)) {
      return new IndexReader(pathname);
    } else {
      return null;
    }
  }

  /**
   * Static function to check if the path contains an index of some type
   */
  public static boolean isIndex(String pathname) throws IOException {
    if (SplitIndexReader.isParallelIndex(pathname)) {
      return true;
    }
    if (IndexReader.isIndexFile(pathname)) {
      return true;
    }
    return false;
  }
}
