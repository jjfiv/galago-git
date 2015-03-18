package org.lemurproject.galago.utility.btree.disk;

import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.utility.buffer.CachedBufferDataStream;
import org.lemurproject.galago.utility.buffer.DataStream;
import org.lemurproject.galago.utility.buffer.ReadableBuffer;
import org.lemurproject.galago.utility.compression.VByte;

import java.io.IOException;

/**
* @author jfoley.
*/
public class DiskBTreeIterator extends BTreeIterator {

  public final ReadableBuffer input;
  private final VocabularyReader vocabulary;
  private final long fileLength;
  private final int cacheGroupSize;

  private VocabularyReader.IndexBlockInfo blockInfo;
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

  public DiskBTreeIterator(DiskBTreeReader reader, VocabularyReader.IndexBlockInfo blockInfo) throws IOException {
    super(reader);
    input = reader.input;
    vocabulary = reader.vocabulary;
    fileLength = reader.fileLength;
    cacheGroupSize = reader.cacheGroupSize;
    this.loadBlockHeader(blockInfo);
  }

  private void loadBlockHeader(VocabularyReader.IndexBlockInfo info) throws IOException {
    this.blockInfo = info;
    long startFileOffset = this.blockInfo.begin;

    // read in a block of data here
    blockStream = new CachedBufferDataStream(input, startFileOffset, blockInfo.headerLength + startFileOffset);

    // now we decode everything from the stream
    this.endValueFileOffset = startFileOffset + blockInfo.length;
    this.keyCount = (int) blockStream.readLong();
    this.keyCache = new byte[this.keyCount][];
    this.endValueOffsetCache = new long[this.keyCount];
    this.startValueFileOffset = startFileOffset + this.blockInfo.headerLength;
    this.keyIndex = 0;
    this.done = false;

    this.cacheKeyCount = 0;
    this.cacheKeys();
  }

  private boolean nextIndexBlock() throws IOException {
    VocabularyReader.IndexBlockInfo nextBlock = vocabulary.getSlot(this.blockInfo.slotId + 1);

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
    if ((CmpUtil.compare(this.blockInfo.firstKey, key) > 0)
            || (CmpUtil.compare(key, this.blockInfo.nextSlotKey) >= 0)) {
      VocabularyReader.IndexBlockInfo newBlock = vocabulary.get(key);
      this.loadBlockHeader(newBlock);
    }

    // since we are 'finding' the key we can move backwards in the current block
    if (CmpUtil.compare(key, keyCache[keyIndex]) < 0) {
      this.keyIndex = 0;
    }

    // now linearly scan the block to find the desired key
    while (keyIndex < keyCount) {
      while (keyIndex >= cacheKeyCount) {
        this.cacheKeys();
      }

      if (CmpUtil.compare(keyCache[keyIndex], key) >= 0) {
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
    if (CmpUtil.compare(key, this.blockInfo.nextSlotKey) >= 0) {
      // restrict the vocab search to only search forward from the current block
      VocabularyReader.IndexBlockInfo newBlock = vocabulary.get(key, this.blockInfo.slotId);
      this.loadBlockHeader(newBlock);
    }

    // now linearly scan the block to find the desired key
    while (keyIndex < keyCount) {
      while (keyIndex >= cacheKeyCount) {
        this.cacheKeys();
      }
      if (CmpUtil.compare(keyCache[keyIndex], key) >= 0) {
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
    return new CachedBufferDataStream(input, getValueStart(), getValueEnd());
  }

  @Override
  public DataStream getSubValueStream(long offset, long length) throws IOException {
    long absoluteStart = getValueStart() + offset;
    long absoluteEnd = getValueStart() + offset + length;

    absoluteEnd = (fileLength < absoluteEnd) ? fileLength : absoluteEnd;
    absoluteEnd = (getValueEnd() < absoluteEnd) ? getValueEnd() : absoluteEnd;

    assert absoluteStart <= absoluteEnd;

    // the end of the sub value is the min of fileLength, valueEnd, or (offset+length);
    return new CachedBufferDataStream(input, absoluteStart, absoluteEnd);
  }

  private void cacheKeys() throws IOException {
    for (int i = 0; i < cacheGroupSize; i++) {
      // if we are done
      if (cacheKeyCount >= keyCount) {
        return;

        // first key
      } else if (this.cacheKeyCount == 0) {
        int keyLength = VByte.uncompressInt(blockStream);
        byte[] keyBytes = new byte[keyLength];
        blockStream.readFully(keyBytes);
        this.keyCache[0] = keyBytes;
        this.endValueOffsetCache[0] = VByte.uncompressInt(blockStream);
        cacheKeyCount++;

        // second or later key
      } else {
        int common = VByte.uncompressInt(blockStream);
        int keyLength = VByte.uncompressInt(blockStream);
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
        this.endValueOffsetCache[cacheKeyCount] = VByte.uncompressInt(blockStream);
        cacheKeyCount++;
      }
    }
  }
}
