// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility.btree.disk;

import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.utility.buffer.CachedBufferDataStream;
import org.lemurproject.galago.utility.buffer.DataStream;
import org.lemurproject.galago.utility.buffer.FileReadableBuffer;
import org.lemurproject.galago.utility.buffer.ReadableBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

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
 * data. For inverted list data it's best to use your own compression, but for
 * text data the GZip compression is a good choice.</p>
 *
 * <p>Typically this class is extended by composition instead of
 * inheritance.</p>
 *
 * <p>(11/29/2010, irmarc): After conferral with Sam, going to remove the
 * requirement that keys be Strings. It makes the mapping from other
 * classes/primitives to Strings really restrictive if they always have to be
 * mapped to Strings. Therefore, mapping byte[] keys to the client keyspace is
 * the responsibility of the client of the DiskBTreeReader.</p>
 *
 * @author trevor
 * @author irmarc
 */
public class DiskBTreeReader extends BTreeReader {

  // this input reader needs to be accesed in a synchronous manner.
  final ReadableBuffer input;

  // other variables do not
  VocabularyReader vocabulary;
  private Parameters manifest;
  int cacheGroupSize = 5;
  long fileLength;


  /**
   * Opens an index found in the buffer.
   *
   * @param buffer  The buffer containing the BTree.
   * @throws IOException
   */
  public DiskBTreeReader(ReadableBuffer buffer) throws IOException {
    input = buffer;

    CachedBufferDataStream inputStream = new CachedBufferDataStream(buffer);

    // Seek to the end of the file
    fileLength = input.length();
    long footerOffset = fileLength - Integer.SIZE / 8 - 3 * Long.SIZE / 8;

    /**
     * In a constructor synchronized is not strictly necessary, no other threads
     * can use this object before it's creation... However, I'm wrapping *all*
     * usage.
     */
    inputStream.seek(footerOffset);
    // Now, read metadata values:
    long vocabularyOffset = inputStream.readLong();
    long manifestOffset = inputStream.readLong();
    int blockSize = inputStream.readInt();
    long magicNumber = inputStream.readLong();
    if (magicNumber != DiskBTreeFormat.MAGIC_NUMBER) {
      throw new IOException("This does not appear to be an index file (wrong magic number)");
    }

    long vocabularyLength = manifestOffset - vocabularyOffset;

    //input.seek(vocabularyOffset);
    vocabulary = new VocabularyReader(new CachedBufferDataStream(input, vocabularyOffset, vocabularyOffset + vocabularyLength), vocabularyOffset);

    ByteBuffer manifestData = ByteBuffer.allocate((int) (footerOffset - manifestOffset));
    input.read(manifestData, manifestOffset);
    manifest = Parameters.parseBytes(manifestData.array());

    this.cacheGroupSize = (int) manifest.get("cacheGroupSize", 1);
  }

  /**
   * Opens an index found in the at pathname.
   *
   * @param pathname Filename of the index to open.
   * @throws FileNotFoundException
   * @throws IOException
   */
  public DiskBTreeReader(String pathname) throws IOException {
    this(new FileReadableBuffer(pathname));
  }

  /**
   * Identical to the {@link #DiskBTreeReader(String) other constructor}, except
   * this one takes a File object instead of a string as the parameter.
   *
   * @throws java.io.IOException
   */
  public DiskBTreeReader(File pathname) throws IOException {
    this(pathname.toString());
  }

  /**
   * Returns a Parameters object that contains metadata about the contents of
   * the index. This is the place to store important data about the index
   * contents, like what stemmer was used or the total number of terms in the
   * collection.
   */
  @Override
  public Parameters getManifest() {
    return manifest;
  }

  /**
   * Returns the vocabulary structure for this DiskBTreeReader. Note that the
   * vocabulary contains only the first key in each block.
   */
  @Override
  public VocabularyReader getVocabulary() {
    return vocabulary;
  }

  /**
   * Returns an iterator pointing to the very first key in the index. This is
   * typically used for iterating through the entire index, which might be
   * useful for testing and debugging tools, but probably not for traditional
   * document retrieval.
   */
  @Override
  public DiskBTreeIterator getIterator() throws IOException {
    // if we have an empty file - there is nothing to iterate over.
    if (manifest.get("emptyIndexFile", false)) {
      return null;
    }

    // otherwise there is some data.
    return new DiskBTreeIterator(this, vocabulary.getSlot(0));
  }

  /**
   * Returns an iterator pointing at a specific key. Returns null if the key is
   * not found in the index.
   */
  @Override
  public DiskBTreeIterator getIterator(byte[] key) throws IOException {
    // read from offset to offset in the vocab structure (right?)
    VocabularyReader.IndexBlockInfo slot = vocabulary.get(key);

    if (slot == null) {
      return null;
    }
    DiskBTreeIterator i = new DiskBTreeIterator(this, slot);
    i.find(key);
    if (CmpUtil.equals(key, i.getKey())) {
      return i;
    }
    return null;
  }

  /**
   * Closes all files associated with the DiskBTreeReader.
   */
  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public DataStream getSpecialStream(long startPosition, long length) throws IOException {
    long absoluteEnd = startPosition + length;
    absoluteEnd = (fileLength < absoluteEnd) ? fileLength : absoluteEnd;

    assert startPosition <= absoluteEnd;

    // the end of the sub value is the min of fileLength, valueEnd, or (offset+length);
    return new CachedBufferDataStream(input, startPosition, absoluteEnd);
  }

  /**
   * Returns true if the file specified by this pathname was probably written by
   * DiskBTreeWriter. If this method returns false, the file is definitely not
   * readable by DiskBTreeReader.
   *
   * @throws java.io.IOException
   */
  public static boolean isBTree(File file) throws IOException {
    RandomAccessFile f = null;
    boolean result = false;
    try {
      f = new RandomAccessFile(file, "r");
      long length = f.length();
      long magicNumber = 0;

      if (length > Long.SIZE / 8) {
        f.seek(length - Long.SIZE / 8);
        magicNumber = f.readLong();
      }
      result = (magicNumber == DiskBTreeFormat.MAGIC_NUMBER);
    } finally {
      if(f != null) {
        f.close();
      }
    }

    return result;
  }
}
