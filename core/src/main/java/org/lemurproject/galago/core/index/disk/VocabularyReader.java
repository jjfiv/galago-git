// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * 
 * @author trevor
 */
public class VocabularyReader {

  public static class IndexBlockInfo {
    public int slotId;
    public byte[] firstKey;
    public byte[] nextSlotKey;
    public long begin;
    public long length;
    public int headerLength;
  }
  List<IndexBlockInfo> slots;

  public VocabularyReader(RandomAccessFile input, long invertedFileLength,
          long vocabularyLength) throws IOException {
    slots = new ArrayList<IndexBlockInfo>();
    read(invertedFileLength, vocabularyLength, input);
  }

  public IndexBlockInfo getSlot(int id) {
    if(id == slots.size()){
      return null;
    }
    return slots.get(id);
  }

  // needed for DocumentSource - should be fixed //
  public List<IndexBlockInfo> getSlots() {
    return slots;
  }

  public void read(long invertedFileLength, long vocabularyLength, RandomAccessFile input) throws IOException {
    long last = 0;
    long start = input.getFilePointer();

    int finalKeyLength = input.readUnsignedByte();
    byte[] finalIndexKey = new byte[finalKeyLength];
    input.read(finalIndexKey);
    
    while (input.getFilePointer() < start + vocabularyLength) {
      int length = Utility.uncompressInt(input);
      //short length = input.readShort();
      byte[] data = new byte[length];
      input.read(data);
      long offset = Utility.uncompressLong(input);
      //long offset = input.readLong();
      int headerLength = Utility.uncompressInt(input);
      //short headerLength = input.readShort();
      IndexBlockInfo slot = new IndexBlockInfo();

      if (slots.size() > 0) {
        slots.get(slots.size() - 1).length = offset - last;
        slots.get(slots.size() - 1).nextSlotKey = data;
      }

      slot.headerLength = headerLength;
      slot.begin = offset;
      slot.firstKey = data;
      slot.slotId = slots.size();

      slots.add(slot);
      last = offset;
    }

    if (slots.size() > 0) {
      slots.get(slots.size() - 1).length = invertedFileLength - last;
      slots.get(slots.size() - 1).nextSlotKey = finalIndexKey;
    }
    assert invertedFileLength >= last;
  }

  public IndexBlockInfo get(byte[] key) {
    return get(key, 0);
  }

  public IndexBlockInfo get(byte[] key, int minBlock) {
    if (slots.size() == 0) {
      return null;
    }
    int big = slots.size() - 1;
    int small = minBlock;

    while (big - small > 1) {
      int middle = small + (big - small) / 2;
      byte[] middleKey = slots.get(middle).firstKey;

      if (Utility.compare(middleKey, key) <= 0) {
        small = middle;
      } else {
        big = middle;
      }
    }

    IndexBlockInfo one = slots.get(small);
    IndexBlockInfo two = slots.get(big);

    if (Utility.compare(two.firstKey, key) <= 0) {
      return two;
    } else {
      return one;
    }
  }
}
