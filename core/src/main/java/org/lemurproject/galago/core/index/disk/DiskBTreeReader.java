// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.disk.VocabularyReader.IndexBlockInfo;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
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
  final private RandomAccessFile input;
  // other variables do not
  private VocabularyReader vocabulary;
  private Parameters manifest;
  private int cacheGroupSize = 5;
  private long fileLength;

  public class Iterator extends BTreeReader.BTreeIterator {

    private IndexBlockInfo blockInfo;
    // key block data
    private long startFileOffset; // start of block
    private long startValueFileOffset; // start of value data
    private long endValueFileOffset; // end of value data / block
    private long[] endValueOffsetCache; // ends of each value data
    private byte[][] keyCache; // keys
    private int keyIndex;
    private int keyCount;
    private boolean done;
    // vars that allow us to avoid reading the entire block
    private DataStream blockStream;
    private int cacheKeyCount;

    public Iterator(BTreeReader reader, IndexBlockInfo blockInfo) throws IOException {
      super(reader);
      this.loadBlockHeader(blockInfo);
    }

    private void loadBlockHeader(IndexBlockInfo info) throws IOException {
      this.blockInfo = info;
      this.startFileOffset = this.blockInfo.begin;

      // read in a block of data here
      blockStream = new BufferedFileDataStream(input, startFileOffset, blockInfo.headerLength + startFileOffset);

      // now we decode everything from the stream
      this.endValueFileOffset = startFileOffset + blockInfo.length;
      this.keyCount = (int) blockStream.readLong();
      this.keyCache = new byte[this.keyCount][];
      this.endValueOffsetCache = new long[this.keyCount];
      this.startValueFileOffset = this.startFileOffset + this.blockInfo.headerLength;
      this.keyIndex = 0;
      this.done = false;

      this.cacheKeyCount = 0;
      this.cacheKeys();
    }

    private boolean nextIndexBlock() throws IOException {
      IndexBlockInfo nextBlock = vocabulary.getSlot(this.blockInfo.slotId + 1);

      // no more vocabulary blocks
      if (nextBlock == null) {
        // set to final key in current block
        this.keyIndex = this.keyCount - 1;
        this.done = true;
        return false;
      }

      // load the new block
      this.loadBlockHeader(nextBlock);
      return true;
    }

    // move functions:
    @Override
    public boolean nextKey() throws IOException {
      this.keyIndex++;
      if (this.keyIndex >= this.keyCount) {
        return this.nextIndexBlock();
      }

      while (keyIndex >= cacheKeyCount) {
        this.cacheKeys();
      }

      return true;
    }

    @Override
    public void find(byte[] key) throws IOException {

      // if the key is not in this block:
      if ((Utility.compare(this.blockInfo.firstKey, key) > 0)
              || (Utility.compare(key, this.blockInfo.nextSlotKey) >= 0)) {
        IndexBlockInfo newBlock = vocabulary.get(key);
        this.loadBlockHeader(newBlock);
      }

      // since we are 'finding' the key we can move backwards in the current block
      if (Utility.compare(key, keyCache[keyIndex]) < 0) {
        this.keyIndex = 0;
      }

      // now linearly scan the block to find the desired key
      while (keyIndex < keyCount) {
        while (keyIndex >= cacheKeyCount) {
          this.cacheKeys();
        }

        if (Utility.compare(keyCache[keyIndex], key) >= 0) {
          // we have found or passed the desired key
          return;
        }
        keyIndex++;
      }

      // if we got here - we have not yet found the correct key
      // this function will ensure we are consistent
      nextKey();
    }

    @Override
    public void skipTo(byte[] key) throws IOException {
      // if the key is not in this block:
      if (Utility.compare(key, this.blockInfo.nextSlotKey) >= 0) {
        // restrict the vocab search to only search forward from the current block
        IndexBlockInfo newBlock = vocabulary.get(key, this.blockInfo.slotId);
        this.loadBlockHeader(newBlock);
      }

      // now linearly scan the block to find the desired key
      while (keyIndex < keyCount) {
        while (keyIndex >= cacheKeyCount) {
          this.cacheKeys();
        }
        if (Utility.compare(keyCache[keyIndex], key) >= 0) {
          // we have found or passed the desired key
          return;
        }
        keyIndex++;
      }
      // if we got here - we have not yet found the correct key
      nextKey();
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public byte[] getKey() {
      return this.keyCache[this.keyIndex];
    }

    @Override
    public long getValueStart() throws IOException {
      return (keyIndex == 0)
              ? startValueFileOffset
              : endValueFileOffset - endValueOffsetCache[keyIndex - 1];
    }

    @Override
    public long getValueEnd() throws IOException {
      return endValueFileOffset - endValueOffsetCache[keyIndex];
    }

    @Override
    public long getValueLength() throws IOException {
      return getValueEnd() - getValueStart();
    }

    @Override
    public DataStream getValueStream() throws IOException {
      return new BufferedFileDataStream(input, getValueStart(), getValueEnd());
    }

    @Override
    public DataStream getSubValueStream(long offset, long length) throws IOException {
      long absoluteStart = getValueStart() + offset;
      long absoluteEnd = getValueStart() + offset + length;
      
      absoluteEnd = (fileLength < absoluteEnd) ? fileLength : absoluteEnd;
      absoluteEnd = (getValueEnd() < absoluteEnd) ? getValueEnd() : absoluteEnd;

      assert absoluteStart <= absoluteEnd;

      // the end of the sub value is the min of fileLength, valueEnd, or (offset+length);
      return new BufferedFileDataStream(input, absoluteStart, absoluteEnd);
    }

    @Override
    public MappedByteBuffer getValueMemoryMap() throws IOException {
      return null;
//      MappedByteBuffer buffer;
//      synchronized (input) {
//        long start = getValueStart();
//        long end = getValueEnd();
//        if(true) return null;
//        try {
//          buffer = input.getChannel().map(MapMode.READ_ONLY, start, end);
//        } catch (IOException e) {
//          System.out.println("Failed to open MemoryMap over key-value" + e.getMessage());
//          throw e;
//        }
//      }
//      return buffer;
    }

    private void cacheKeys() throws IOException {
      for (int i = 0; i < cacheGroupSize; i++) {
        // if we are done
        if (cacheKeyCount >= keyCount) {
          return;

          // first key
        } else if (this.cacheKeyCount == 0) {
          int keyLength = Utility.uncompressInt(blockStream);
          byte[] keyBytes = new byte[keyLength];
          blockStream.readFully(keyBytes);
          this.keyCache[0] = keyBytes;
          this.endValueOffsetCache[0] = Utility.uncompressInt(blockStream);
          cacheKeyCount++;

          // second or later key
        } else {
          int common = Utility.uncompressInt(blockStream);
          int keyLength = Utility.uncompressInt(blockStream);
          assert keyLength >= 0 : "Negative key length: " + keyLength + " " + cacheKeyCount;
          assert keyLength >= common : "key length too small: " + keyLength + " " + common + " " + cacheKeyCount;
          byte[] keyBytes = new byte[keyLength];

          try {
            System.arraycopy(keyCache[cacheKeyCount - 1], 0, keyBytes, 0, common);
            blockStream.readFully(keyBytes, common, keyLength - common);
          } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("wl: " + keyLength + " c: " + common);
            throw e;
          }
          this.keyCache[cacheKeyCount] = keyBytes;
          this.endValueOffsetCache[cacheKeyCount] = Utility.uncompressInt(blockStream);
          cacheKeyCount++;
        }
      }
    }
  }

  /**
   * Opens an index found in the at pathname.
   *
   * @param pathname Filename of the index to open.
   * @throws FileNotFoundException
   * @throws IOException
   */
  public DiskBTreeReader(String pathname) throws IOException {
    input = StreamCreator.readFile(pathname);

    // Seek to the end of the file
    fileLength = input.length();
    long footerOffset = fileLength - Integer.SIZE / 8 - 3 * Long.SIZE / 8;

    /**
     * In a constructor synchronized is not strictly necessary, no other threads
     * can use this object before it's creation... However, I'm wrapping *all*
     * usage.
     */
    synchronized (input) {
      input.seek(footerOffset);
      // Now, read metadata values:
      long vocabularyOffset = input.readLong();
      long manifestOffset = input.readLong();
      int blockSize = input.readInt();
      long magicNumber = input.readLong();
      if (magicNumber != DiskBTreeWriter.MAGIC_NUMBER) {
        throw new IOException("This does not appear to be an index file (wrong magic number)");
      }

      long invertedListLength = vocabularyOffset;
      long vocabularyLength = manifestOffset - vocabularyOffset;

      //input.seek(vocabularyOffset);
      vocabulary = new VocabularyReader(new BufferedFileDataStream(input, vocabularyOffset, vocabularyOffset + vocabularyLength), invertedListLength);

      byte[] manifestData = new byte[(int) (footerOffset - manifestOffset)];
      input.seek(manifestOffset);
      input.readFully(manifestData);
      manifest = Parameters.parseBytes(manifestData);
    }

    this.cacheGroupSize = (int) manifest.get("cacheGroupSize", 1);
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
  public DiskBTreeReader.Iterator getIterator() throws IOException {
    // if we have an empty file - there is nothing to iterate over.
    if (manifest.get("emptyIndexFile", false)) {
      return null;
    }

    // otherwise there is some data.
    return new Iterator(this, vocabulary.getSlot(0));
  }

  /**
   * Returns an iterator pointing at a specific key. Returns null if the key is
   * not found in the index.
   */
  @Override
  public DiskBTreeReader.Iterator getIterator(byte[] key) throws IOException {
    // read from offset to offset in the vocab structure (right?)
    VocabularyReader.IndexBlockInfo slot = vocabulary.get(key);

    if (slot == null) {
      return null;
    }
    Iterator i = new Iterator(this, slot);
    i.find(key);
    if (Utility.compare(key, i.getKey()) == 0) {
      return i;
    }
    return null;
  }

  /**
   * Closes all files associated with the DiskBTreeReader.
   */
  @Override
  public void close() throws IOException {
    //synchronized (input) {
    input.close();
    //}
  }

  @Override
  public DataStream getSpecialStream(long startPosition, long length) {
    long absoluteEnd = startPosition + length;
    absoluteEnd = (fileLength < absoluteEnd) ? fileLength : absoluteEnd;

    assert startPosition <= absoluteEnd;

    // the end of the sub value is the min of fileLength, valueEnd, or (offset+length);
    return new BufferedFileDataStream(input, startPosition, absoluteEnd);
  }

  /**
   * ***********
   */
  // local functions
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
      result = (magicNumber == DiskBTreeWriter.MAGIC_NUMBER);
    } finally {
      if(f != null) {
        f.close();
      }
    }

    return result;
  }
}
