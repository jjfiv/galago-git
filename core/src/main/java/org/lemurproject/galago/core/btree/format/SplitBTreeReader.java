// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.btree.format;

import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamCreator;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.utility.btree.disk.DiskBTreeIterator;
import org.lemurproject.galago.utility.btree.disk.DiskBTreeReader;
import org.lemurproject.galago.utility.btree.disk.GalagoBTreeReader;
import org.lemurproject.galago.utility.btree.disk.VocabularyReader;
import org.lemurproject.galago.utility.buffer.CachedBufferDataStream;
import org.lemurproject.galago.utility.buffer.DataStream;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Split index reader
 *  - Index is a mapping from byte[] to byte[]
 *
 *  - allows values to be written out of order to a set of files
 *  - a unified ordered key structure should be kept in a folder
 *    with these value files, as created by SplitIndexKeyWriter
 *  - SplitBTreeReader will read this data
 *
 *  This class if useful for writing a corpus structure
 *  - documents can be written to disk in any order
 *  - the key structure allows the documents to be found quickly
 *  - class is more efficient if the
 *    documents are inserted in sorted order
 *
 * @author sjh
 */
public class SplitBTreeReader extends GalagoBTreeReader {

  public static final long VALUE_FILE_MAGIC_NUMBER = 0x2b3c4d5e6f7a8b9cL;
  RandomAccessFile[] dataFiles;
  DiskBTreeReader vocabIndex;
  String indexFolder;
  int hashMod;

  public class Iterator extends BTreeIterator {

    DiskBTreeIterator vocabIterator;
    boolean valueLoaded = false;
    SplitBTreeKeyInfo info;

    public Iterator(DiskBTreeIterator vocabIterator) {
      super(vocabIterator.reader);
      this.vocabIterator = vocabIterator;
    }

    RandomAccessFile getFile() {
      return dataFiles[info.valueOutputId];
    }

    /**
     * Returns the key associated with the current inverted list.
     */
    @Override
    public byte[] getKey() {
      return vocabIterator.getKey();
    }

    /*
     * Skip iterator to the provided key
     */
    @Override
    public void find(byte[] key) throws IOException {
      vocabIterator.find(key);
      valueLoaded = false;
    }

    /*
     * Skip iterator to the provided key
     */
    @Override
    public void skipTo(byte[] key) throws IOException {
      vocabIterator.skipTo(key);
      valueLoaded = false;
    }

    /**
     * Advances to the next key in the index.
     */
    @Override
    public boolean nextKey() throws IOException {
      valueLoaded = false;
      return vocabIterator.nextKey();
    }

    /**
     * Returns true if no more keys remain to be read.
     */
    @Override
    public boolean isDone() {
      return vocabIterator.isDone();
    }

    /**
     * Returns the length of the value, in bytes.
     */
    @Override
    public long getValueLength() throws IOException {
      if (!valueLoaded) {
        loadValue();
      }

      return info.valueLength;
    }

    /**
     * Returns the value as a buffered stream.
     */
    @Override
    public DataStream getValueStream() throws IOException {
      if (!valueLoaded) {
        loadValue();
      }

      return new CachedBufferDataStream(getFile(), getValueStart(), getValueEnd());
    }



    /**
     * Returns the value as a buffered stream.
     */
    @Override
    public DataStream getSubValueStream(long offset, long length) throws IOException {
      if (!valueLoaded) {
        loadValue();
      }

      long absoluteStart = getValueStart() + offset;
      long absoluteEnd = absoluteStart + length;
      absoluteEnd = Math.min(absoluteEnd, Math.min(getValueEnd(), getFile().length()));

      assert absoluteStart <= absoluteEnd;

      return new CachedBufferDataStream(getFile(), absoluteStart, absoluteEnd);
    }

    /**
     * Returns the byte offset
     * of the beginning of the current inverted list,
     * relative to the start of the whole inverted file.
     */
    @Override
    public long getValueStart() throws IOException {
      if (!valueLoaded) {
        loadValue();
      }
      return info.valueOffset;
    }

    /**
     * Returns the byte offset
     * of the end of the current inverted list,
     * relative to the start of the whole inverted file.
     */
    @Override
    public long getValueEnd() throws IOException {
      if (!valueLoaded) {
        loadValue();
      }
      return info.valueOffset + info.valueLength;
    }

    //**********************//
    // local functions
    /**
     * Reads the header information for a data value
     *
     * @throws IOException
     */
    private void loadValue() throws IOException {
      valueLoaded = true;

      info = SplitBTreeKeyInfo.codec.fromBytes(vocabIterator.getValueBytes());

      if (getFile() == null) {
        dataFiles[info.valueOutputId] = StreamCreator.readFile(indexFolder + File.separator + info.valueOutputId);
      }
    }
  }

  /*
   * Constructors
   */
  public SplitBTreeReader(File f) throws IOException {
    if (f.isDirectory()) {
      f = new File(f.getAbsolutePath() + File.separator + "split.keys");
    }
    vocabIndex = new DiskBTreeReader(f);

    indexFolder = f.getParent();
    //  (-1) for the key index
    hashMod = f.getParentFile().list().length - 1;

    dataFiles = new RandomAccessFile[hashMod];
  }

  /**
   * Returns a Parameters object that contains metadata about
   * the contents of the index.  This is the place to store important
   * data about the index contents, like what stemmer was used or the
   * total number of terms in the collection.
   */
  @Override
  public Parameters getManifest() {
    return vocabIndex.getManifest();
  }

  /**
   * Returns the vocabulary structure for this DiskBTreeReader.  Note that the vocabulary
   * contains only the first key in each block.
   */
  @Override
  public VocabularyReader getVocabulary() {
    return vocabIndex.getVocabulary();
  }

  /**
   * Returns an iterator pointing to the very first key in the index.
   * This is typically used for iterating through the entire index,
   * which might be useful for testing and debugging tools, but probably
   * not for traditional document retrieval.
   */
  @Override
  public Iterator getIterator() throws IOException {
    return new Iterator(vocabIndex.getIterator());
  }

  /**
   * Returns an iterator pointing at a specific key.  Returns
   * null if the key is not found in the index.
   */
  @Override
  public Iterator getIterator(byte[] key) throws IOException {
    DiskBTreeIterator i = vocabIndex.getIterator(key);
    if (i == null) {
      return null;
    } else {
      return new Iterator(i);
    }
  }

  @Override
  public void close() throws IOException {
    vocabIndex.close();
    for (RandomAccessFile f : dataFiles) {
      if (f != null) {
        f.close();
      }
    }
  }

  public static boolean isBTree(File f) throws IOException {
    assert f.exists() : "Path not found: " + f.getAbsolutePath();

    File keys;
    File data;

    if (f.isDirectory()) {
      keys = new File(f, "split.keys");
      data = new File(f, "0");
    } else {
      keys = f;
      data = new File(f.getParentFile(), "0");
    }

    long magic = 0;
    if (keys.exists()
            && data.exists()
            && DiskBTreeReader.isBTree(keys)) {
      RandomAccessFile reader = StreamCreator.readFile(data.getAbsolutePath());
      reader.seek(reader.length() - 8);
      magic = reader.readLong();
      reader.close();
      if (magic == VALUE_FILE_MAGIC_NUMBER) {
        return true;
      }
    }
    return false;
  }
}
