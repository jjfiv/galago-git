// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.lemurproject.galago.core.index.GenericIndexWriter;
import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * This class writes index files, which are used for most Galago indexes.
 * 
 * An index is a mapping between a key and a value, much like a TreeMap.  The keys are
 * sorted to allow iteration over the whole file.  Keys are stored using prefix
 * compression to save space.  The structure is designed for fast random access on disk.
 * 
 * For indexes, we assume that the data in each value is already compressed, so IndexWriter
 * does no additional compression.  However, if the isCompressed flag is set, IndexWriter
 * will compress the value data.  This is convenient for storing documents in an index.
 * 
 * Keys cannot be longer than 256 bytes, and they must be added in sorted order.
 * 
 * @author trevor
 */
public class IndexWriter extends GenericIndexWriter {

  public static final long MAGIC_NUMBER = 0x1a2b3c4d5e6f7a8bL;
  private DataOutputStream output;
  private VocabularyWriter vocabulary;
  private Parameters manifest;
  private ArrayList<IndexElement> lists;
  private int blockSize = 32768;
  private int keySize = 256;
  private long filePosition = 0;
  private long listBytes = 0;
  private long keyCount = 0;
  private byte[] lastKey = new byte[0];
  Counter recordsWritten = null;
  Counter blocksWritten = null;

  /**
   * Creates a new instance of IndexWriter
   */
  public IndexWriter(String outputFilename, Parameters parameters)
          throws FileNotFoundException, IOException {
    Utility.makeParentDirectories(outputFilename);

    // max = unsigned short - 32k
    blockSize = (int) parameters.get("blockSize", 32768);

    // max = unsigned byte - 256
    keySize = 256;
    output = new DataOutputStream(new BufferedOutputStream(
            new FileOutputStream(outputFilename)));
    vocabulary = new VocabularyWriter();
    manifest = new Parameters();
    manifest.copyFrom(parameters);
    lists = new ArrayList<IndexElement>();
  }

  public IndexWriter(String outputFilename)
          throws FileNotFoundException, IOException {
    Utility.makeParentDirectories(outputFilename);
    output = new DataOutputStream(new BufferedOutputStream(
            new FileOutputStream(outputFilename)));
    vocabulary = new VocabularyWriter();
    manifest = new Parameters();
    lists = new ArrayList<IndexElement>();
  }

  public IndexWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    this(parameters.getJSON().getString("filename"), parameters.getJSON());
    recordsWritten = parameters.getCounter("Records Written");
    blocksWritten = parameters.getCounter("Blocks Written");
  }

  /**
   * Returns the current copy of the manifest, which will be stored in
   * the completed index file.  This data is not written until close() is called.
   */
  public Parameters getManifest() {
    return manifest;
  }

  public void add(IndexElement list) throws IOException {
    if (list.key().length >= this.keySize || list.key().length >= blockSize / 4) {
      throw new IOException(String.format("Key %s is too long.", Utility.toString(list.key())));
    }
    if (needsFlush(list)) {
      flush();
    }
    lists.add(list);
    updateBufferedSize(list);
    if (recordsWritten != null) {
      recordsWritten.increment();
    }

    keyCount++;
  }

  public void close() throws IOException {
    flush();

    // increment the final key (this writes the first key that is outside the index.
    lastKey = increment(lastKey);
    assert (lastKey.length < 256) : "Final key issue - can not be written.";

    byte[] vocabularyData = vocabulary.data();
    if (vocabularyData.length == 0) {
      manifest.set("emptyIndexFile", true);
    }
    manifest.set("keyCount", this.keyCount);

    byte[] xmlData = manifest.toString().getBytes("UTF-8");
    long vocabularyOffset = filePosition;
    long manifestOffset = filePosition
            + 1 + lastKey.length // part of vocab
            + vocabularyData.length;

    // need to write an int here - key could be very large.
    output.writeByte(lastKey.length);
    output.write(lastKey);

    output.write(vocabularyData);
    output.write(xmlData);

    output.writeLong(vocabularyOffset);
    output.writeLong(manifestOffset);
    output.writeInt(blockSize);
    output.writeLong(MAGIC_NUMBER);

    output.close();
  }

  // Private functions
  private void writeBlock(List<IndexElement> blockLists, long length) throws IOException {
    assert length <= blockSize || blockLists.size() == 1;
    assert wordsInOrder(blockLists);

    if (blockLists.size() == 0) {
      return;
    }

    // -- compute the length of the block --
    ListData listData = new ListData(blockLists);
    
    // create header data
    byte[] headerBytes = getBlockHeader(blockLists);

    long startPosition = filePosition;
    long endPosition = filePosition + headerBytes.length + listData.encodedLength();
    assert endPosition <= startPosition + length;
    assert endPosition > startPosition;
    assert filePosition >= Integer.MAX_VALUE || filePosition == output.size();

    // -- begin writing the block --
    vocabulary.add(blockLists.get(0).key(), startPosition, headerBytes.length);

    // key data
    output.write(headerBytes);

    // write inverted list binary data
    listData.write(output);

    filePosition = endPosition;
    assert filePosition >= Integer.MAX_VALUE || filePosition == output.size();
    assert endPosition - startPosition <= blockSize || blockLists.size() == 1;

    if (blocksWritten != null) {
      blocksWritten.increment();
    }
  }

  /**
   * Gives a conservative estimate of the buffered size of the data,
   * excluding the most recent inverted list.
   * Does not include savings due to key overlap compression.
   */
  private long bufferedSize() {
    return listBytes + 8; // key count;
  }

  private void updateBufferedSize(IndexElement list) {
    listBytes += invertedListLength(list);
  }

  private long invertedListLength(IndexElement list) {
    long listLength = 0;

    listLength += list.key().length;
    listLength += 1; // key overlap
    listLength += 1; // key length
    listLength += 2; // file offset bytes

    listLength += list.dataLength();
    return listLength;
  }

  /**
   * Flush all lists out to disk.
   */
  private void flush() throws IOException {
    // if there aren't any lists, quit now
    if (lists.size() == 0) {
      return;        // write everything out
    }
    writeBlock(lists, bufferedSize());

    // remove all of the current data
    lists = new ArrayList<IndexElement>();
    listBytes = 0;
  }

  private boolean needsFlush(IndexElement list) {
    long listExtra = 1 + // byte for key length
            1;  // byte for overlap with previous key

    long bufferedBytes = bufferedSize()
            + invertedListLength(list)
            + listExtra;

    return bufferedBytes >= blockSize;
  }

  /**
   * sjh: function generates a key greater than the input key
   *      - this is necessary to ensure the 'final key
   *      - it may increase the length of the key
   */
  private byte[] increment(byte[] key) {
    byte[] newData = Arrays.copyOf(key, key.length);
    int i = newData.length - 1;
    while (i >= 0 && newData[i] == Byte.MAX_VALUE) {
      i--;
    }
    if (i >= 0) {
      newData[i]++;
    } else {
      newData = Arrays.copyOf(key, key.length + 1);
    }
    assert (Utility.compare(key, newData) < 0);
    return newData;
  }

  private boolean lessThanOrEqualTo(byte[] one, byte[] two) {
    boolean isOneShorterOrEqualLength = (one.length <= two.length);
    int commonLength = Math.min(one.length, two.length);

    for (int i = 0; i < commonLength; i++) {
      int a = one[i];
      int b = two[i];
      a &= 0xFF;
      b &= 0xFF;
      if (a < b) {
        return true;
      }
      if (b < a) {
        return false;
      }
    }

    return isOneShorterOrEqualLength;
  }

  /**
   * Returns true if the lists are sorted in ascending order by
   * key.
   * 
   * @param blockLists
   */
  private boolean wordsInOrder(List<IndexElement> blockLists) {
    for (int i = 0; i < blockLists.size() - 1; i++) {
      boolean result = lessThanOrEqualTo(blockLists.get(i).key(),
              blockLists.get(i + 1).key());
      if (result == false) {
        return false;
      }
    }
    return true;
  }

  private int prefixOverlap(byte[] firstTerm, byte[] lastTerm) {
    int maximum = Math.min(firstTerm.length, lastTerm.length);
    maximum = Math.min(Byte.MAX_VALUE - 1, maximum);

    for (int i = 0; i < maximum; i++) {
      if (firstTerm[i] != lastTerm[i]) {
        return i;
      }
    }
    return maximum;
  }

  private byte[] getBlockHeader(List<IndexElement> blockLists) throws IOException {
    ListData listData;
    ArrayList<byte[]> keys;
    short[] ends;
    ByteArrayOutputStream wordByteStream = new ByteArrayOutputStream();
    DataOutputStream vocabOutput = new DataOutputStream(wordByteStream);

    listData = new ListData(blockLists);
    keys = new ArrayList<byte[]>();
    for (IndexElement list : blockLists) {
      keys.add(list.key());
    }

    vocabOutput.writeLong(keys.size());
    long totalListData = listData.length();
    long invertedListBytes = 0;

    byte[] word = listData.blockLists.get(0).key();
    byte[] lastWord = word;
    assert word.length < this.keySize;

    // this is the first word in the block
    vocabOutput.writeByte(word.length);
    vocabOutput.write(word, 0, word.length);

    invertedListBytes += listData.blockLists.get(0).dataLength();
    assert totalListData - invertedListBytes < this.blockSize;
    assert totalListData >= invertedListBytes;
    vocabOutput.writeShort((int) (totalListData - invertedListBytes));

    for (int j = 1; j < keys.size(); j++) {
      assert word.length < this.keySize;
      word = listData.blockLists.get(j).key();
      int common = this.prefixOverlap(lastWord, word);
      vocabOutput.writeByte((byte) common);
      vocabOutput.writeByte(word.length);
      vocabOutput.write(word, common, word.length - common);
      invertedListBytes += listData.blockLists.get(j).dataLength();
      assert totalListData - invertedListBytes < this.blockSize;
      assert totalListData >= invertedListBytes;
      vocabOutput.writeShort((int) (totalListData - invertedListBytes));
      lastWord = word;
    }
    vocabOutput.close();

    return wordByteStream.toByteArray();
  }

  // private class to hold a list of index elements (key-value_ pairs)
  private static class ListData {

    List<IndexElement> blockLists;

    public ListData(List<IndexElement> blockLists) {
      this.blockLists = blockLists;
    }

    public long length() {
      long totalLength = 0;
      for (IndexElement e : blockLists) {
        totalLength += e.dataLength();
      }
      return totalLength;
    }

    public long encodedLength() {
      return length();
    }

    public void write(OutputStream stream) throws IOException {
      for (IndexElement e : blockLists) {
        e.write(stream);
      }
    }
  }
}
