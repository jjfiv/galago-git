// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility.btree;

import org.lemurproject.galago.utility.btree.disk.VocabularyReader;
import org.lemurproject.galago.utility.buffer.DataStream;
import org.lemurproject.galago.utility.Parameters;

import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;

/**
 * <p>This implements the core functionality for all inverted list readers. It
 * can also be used as a read-only TreeMap for disk-based data structures. In
 * Galago, it is used both to store index data and to store documents.</p>
 *
 * <p>An index is a mapping from String to byte[]. If compression is turned on,
 * the value must be small enough that it fits in memory. If compression is off,
 * values are streamed directly from disk so there is no size restriction.
 * Indexes support iteration over all keys, or direct lookup of a single key.
 * The structure is optimized to support fast random lookup on disks.</p>
 *
 * <p>Data is stored in blocks, typically 32K each. Each block has a
 * prefix-compressed set of keys at the beginning, followed by a block of value
 * data. </p>
 *
 * <p>Typically this class is extended by composition instead of
 * inheritance.</p>
 *
 * <p> (11/29/2010, irmarc): After conferral with Sam, going to remove the
 * requirement that keys be Strings. It makes the mapping from other
 * classes/primitives to Strings really restrictive if they always have to be
 * mapped to Strings. Therefore, mapping byte[] keys to the client keyspace is
 * the responsibility of the client of the DiskBTreeReader.</p>
 *
 * Comments copied from DiskBTreeReader: author trevor, irmarc, sjh
 *
 * @author sjh
 */
public abstract class BTreeReader implements Closeable {

  // Abstract functions
  /**
   * Returns a Parameters object that contains metadata about the contents of
   * the index. This is the place to store important data about the index
   * contents, like what stemmer was used or the total number of terms in the
   * collection.
   */
  public abstract Parameters getManifest();

  /**
   * Returns the vocabulary structure for this DiskBTreeReader. - Note that the
   * vocabulary contains only the first key in each block.
   */
  public abstract VocabularyReader getVocabulary();

  /**
   * Returns an iterator pointing to the very first key in the index. This is
   * typically used for iterating through the entire index, which might be
   * useful for testing and debugging tools, but probably not for traditional
   * document retrieval.
   */
  public abstract BTreeIterator getIterator() throws IOException;

  /**
   * Returns an iterator pointing at a specific key. Returns null if the key is
   * not found in the index.
   */
  public abstract BTreeIterator getIterator(byte[] key) throws IOException;

  /**
   * Closes all files associated with the DiskBTreeReader.
   */
  public abstract void close() throws IOException;

  // Implemented functions
  /**
   * Returns the value stored in the index associated with this key.
   */
  public String getValueString(byte[] key) throws IOException {
    BTreeIterator iter = getIterator(key);

    if (iter == null) {
      return null;
    }

    return iter.getValueString();
  }

  /**
   * Returns the value stored in the index associated with this key.
   */
  public byte[] getValueBytes(byte[] key) throws IOException {
    BTreeIterator iter = getIterator(key);

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
    BTreeIterator iter = getIterator(key);

    if (iter == null) {
      return null;
    }
    return iter.getValueStream();
  }

  /**
   * Gets the value stored in the index associated with this key.
   *
   * @param key
   * @return The index value for this key, or null if there is no such value.
   * @throws java.io.IOException
   */
  public MappedByteBuffer getValueMemoryMap(byte[] key) throws IOException {
    BTreeIterator iter = getIterator(key);

    if (iter == null) {
      return null;
    }
    return iter.getValueMemoryMap();
  }

  public abstract DataStream getSpecialStream(long startPosition, long length) throws IOException;
}
