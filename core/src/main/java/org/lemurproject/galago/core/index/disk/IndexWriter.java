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
  DataOutputStream output;
  final VocabularyWriter vocabulary;
  Parameters manifest;
  ArrayList<IndexElement> lists;
  int blockSize = 32768;
  int vocabGroup = 16;
  long filePosition = 0;
  long listBytes = 0;
  long keyCount = 0;
  // compression isn't supported yet
  boolean isCompressed = false;
  Counter recordsWritten = null;
  Counter blocksWritten = null;

  /**
   * Creates a new instance of IndexWriter
   */
  public IndexWriter(String outputFilename, Parameters parameters)
          throws FileNotFoundException, IOException {
    Utility.makeParentDirectories(outputFilename);
    blockSize = (int) parameters.get("blockSize", 32768);
    isCompressed = parameters.get("isCompressed", false);
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

  /** 
   * Gives a conservative estimate of the buffered size of the data,
   * excluding the most recent inverted list.
   * Does not include savings due to key overlap compression.
   */
  public long bufferedSize() {
    long extra = 8 + // end of block
            8 + // key count
            1; // overlap length

    return listBytes + extra;
  }

  public void updateBufferedSize(IndexElement list) {
    long extra = 1 + // byte for key length
            1;  // byte for overlap with previous key

    listBytes += invertedListLength(list);
    listBytes += extra;
  }

  private long invertedListLength(IndexElement list) {
    long listLength = 0;

    listLength += list.key().length;
    listLength += 2; // key length bytes
    listLength += 2; // file offset bytes

    listLength += list.dataLength();
    return listLength;
  }

  /**
   * Flush all lists out to disk.
   */
  public void flush() throws IOException {
    // if there aren't any lists, quit now
    if (lists.size() == 0) {
      return;        // write everything out
    }
    writeBlock(lists, bufferedSize());

    // remove all of the current data
    lists = new ArrayList<IndexElement>();
    listBytes = 0;
  }

  public long getBlockSize() {
    return blockSize;
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
  public boolean wordsInOrder(List<IndexElement> blockLists) {
    for (int i = 0; i < blockLists.size() - 1; i++) {
      boolean result = lessThanOrEqualTo(blockLists.get(i).key(),
              blockLists.get(i + 1).key());
      if (result == false) {
        return false;
      }
    }
    return true;
  }

  interface ListData {

    long length();

    long encodedLength();

    void write(OutputStream stream) throws IOException;
  }

  class UncompressedListData implements ListData {

    List<IndexElement> blockLists;

    UncompressedListData(List<IndexElement> blockLists) {
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

  class CompressedListData implements ListData {

    List<IndexElement> blockLists;
    byte[] compressedData;

    CompressedListData(List<IndexElement> blockLists) throws IOException {
      this.blockLists = blockLists;
      compress();
    }

    void compress() throws IOException {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();

      // write the uncompressed length here
      DataOutputStream s = new DataOutputStream(stream);
      s.writeInt((int) length());

      GZIPOutputStream gzipStream = new GZIPOutputStream(stream);
      for (IndexElement element : blockLists) {
        element.write(gzipStream);
      }

      gzipStream.close();
      compressedData = stream.toByteArray();
    }

    public long length() {
      long totalLength = 0;
      for (IndexElement e : blockLists) {
        totalLength += e.dataLength();
      }
      return totalLength;
    }

    public long encodedLength() {
      return compressedData.length;
    }

    public void write(OutputStream stream) throws IOException {
      stream.write(compressedData);
    }
  }

  static class VocabularyHeader {

    ArrayList<byte[]> keys;
    short[] ends;
    ByteArrayOutputStream wordByteStream = new ByteArrayOutputStream();
    DataOutputStream vocabOutput = new DataOutputStream(wordByteStream);
    int blockOverlap;
    int groupCount;
    int vocabGroupSize;

    VocabularyHeader(List<IndexElement> blockLists, int vocabGroupSize) {
      keys = new ArrayList<byte[]>();
      this.vocabGroupSize = vocabGroupSize;
      for (IndexElement list : blockLists) {
        keys.add(list.key());
      }
    }

    int prefixOverlap(byte[] firstTerm, byte[] lastTerm, int start) {
      int maximum = Math.min(firstTerm.length - start, lastTerm.length - start);
      maximum = Math.min(Byte.MAX_VALUE - 1, maximum);

      for (int i = start; i < maximum; i++) {
        if (firstTerm[i] != lastTerm[i]) {
          return i - start;
        }
      }

      return maximum;
    }

    int prefixOverlap(byte[] firstTerm, byte[] secondTerm) {
      return prefixOverlap(firstTerm, secondTerm, 0);
    }

    void calculateBlockPrefix() {
      // vocabulary group (prefix sharing)
      byte[] firstWord = keys.get(0);
      byte[] lastWord = keys.get(keys.size() - 1);

      // determine how many prefix characters are in common among all terms in this block
      blockOverlap = prefixOverlap(firstWord, lastWord);
    }

    void build() throws IOException {
      calculateBlockPrefix();

      groupCount = (int) Math.ceil((float) keys.size() / vocabGroupSize);
      ends = new short[groupCount];

      // write key data: outer loop is for each vocabulary group
      for (int i = 0; i < keys.size(); i += vocabGroupSize) {
        byte[] word = keys.get(i);
        byte[] lastWord = word;
        assert word.length >= blockOverlap :
                "Overlap: " + blockOverlap + " too small for " + word.length
                + " (" + Utility.toString(word) + ")";
        assert word.length < 256;

        // this is the first word in the group
        vocabOutput.writeByte(word.length - blockOverlap);
        vocabOutput.write(word, blockOverlap, word.length - blockOverlap);
        int end = Math.min(keys.size(), i + vocabGroupSize);

        // inner loop is for the remaining terms in each vocabulary group
        for (int j = i + 1; j < end; j++) {
          assert word.length < 256;

          // write only new data (reference the previous key for prefix compression)
          word = keys.get(j);
          int common = this.prefixOverlap(lastWord, word);
          vocabOutput.writeByte((byte) common);
          vocabOutput.writeByte(word.length);
          vocabOutput.write(word, common, word.length - common);
          lastWord = word;
        }

        ends[i / vocabGroupSize] = (short) vocabOutput.size();
      }
      vocabOutput.close();
    }

    int getBlockOverlap() {
      return blockOverlap;
    }

    int getGroupCount() {
      return groupCount;
    }

    int getKeyCount() {
      return keys.size();
    }

    int getKeyDataLength() {
      return wordByteStream.size();
    }

    byte[] getFirstWord() {
      return keys.get(0);
    }

    void writeKeyHeader(DataOutputStream output) throws IOException {
      // write key count
      output.writeLong(getKeyCount());

      // write key prefix
      output.writeByte((byte) blockOverlap);
      output.write(getFirstWord(), 0, blockOverlap);

      // write key block lengths
      for (short wordBlockEnd : ends) {
        output.writeShort(wordBlockEnd);
      }
    }

    void writeKeyData(DataOutputStream output) throws IOException {
      output.write(wordByteStream.toByteArray());
    }
  }

  public void writeBlock(List<IndexElement> blockLists, long length) throws IOException {
    assert length <= blockSize || blockLists.size() == 1;
    assert wordsInOrder(blockLists);

    if (blockLists.size() == 0) {
      return;
    }

    VocabularyHeader vocabHeader = new VocabularyHeader(blockLists, vocabGroup);
    vocabHeader.build();

    // -- compute the length of the block --
    ListData listData;
    if (isCompressed) {
      listData = new CompressedListData(blockLists);
    } else {
      listData = new UncompressedListData(blockLists);
    }

    long headerBytes = 8 + // key count
            8 + // block end
            1 + vocabHeader.getBlockOverlap() + // key prefix bytes
            2 * vocabHeader.getGroupCount() + // key lengths 
            2 * vocabHeader.getKeyCount() + // inverted list endings
            vocabHeader.getKeyDataLength();    // key data 

    long startPosition = filePosition;
    long endPosition = filePosition + headerBytes + listData.encodedLength();
    assert endPosition <= startPosition + length || isCompressed;
    assert endPosition > startPosition || isCompressed;
    assert filePosition >= Integer.MAX_VALUE || filePosition == output.size();

    // -- begin writing the block -- 
    vocabulary.add(vocabHeader.getFirstWord(), startPosition);

    // write block data end
    output.writeLong(endPosition);
    vocabHeader.writeKeyHeader(output);

    // write inverted list end positions
    long totalListData = listData.length();
    long invertedListBytes = 0;
    for (IndexElement list : blockLists) {
      invertedListBytes += list.dataLength();
      assert totalListData - invertedListBytes < Short.MAX_VALUE;
      assert totalListData >= invertedListBytes;
      output.writeShort((short) (totalListData - invertedListBytes));
    }

    // key data
    vocabHeader.writeKeyData(output);

    // write inverted list binary data
    listData.write(output);

    filePosition = endPosition;
    assert filePosition >= Integer.MAX_VALUE || filePosition == output.size();
    assert endPosition - startPosition <= blockSize || blockLists.size() == 1 || isCompressed;

    if (blocksWritten != null) {
      blocksWritten.increment();
    }
  }

  private boolean needsFlush(IndexElement list) {
    long listExtra = 1 + // byte for key length
            1;  // byte for overlap with previous key

    long bufferedBytes = bufferedSize()
            + invertedListLength(list)
            + listExtra;

    return bufferedBytes >= blockSize;
  }

  public void add(IndexElement list) throws IOException {
    if (list.key().length >= 256 || list.key().length >= blockSize / 4) {
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

    byte[] vocabularyData = vocabulary.data();
    if (vocabularyData.length == 0) {
      manifest.set("emptyIndexFile", true);
    }
    manifest.set("keyCount", this.keyCount);

    byte[] xmlData = manifest.toString().getBytes("UTF-8");
    long vocabularyOffset = filePosition;
    long manifestOffset = filePosition + vocabularyData.length;

    output.write(vocabularyData);
    output.write(xmlData);

    output.writeLong(vocabularyOffset);
    output.writeLong(manifestOffset);
    output.writeInt(blockSize);
    output.writeInt(vocabGroup);
    output.writeBoolean(isCompressed);
    output.writeLong(MAGIC_NUMBER);

    output.close();
  }
}
