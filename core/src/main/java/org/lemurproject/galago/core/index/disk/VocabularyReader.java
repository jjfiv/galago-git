// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * 
 * @author trevor
 */
public class VocabularyReader {

  public static class TermSlot {

    public byte[] termData;
    public long begin;
    public long length;
  }
  ArrayList<TermSlot> slots;

  public VocabularyReader(RandomAccessFile input, long invertedFileLength,
          long vocabularyLength) throws IOException {
    slots = new ArrayList<TermSlot>();
    read(invertedFileLength, vocabularyLength, input);
  }

  public ArrayList<TermSlot> getSlots() {
    return slots;
  }

  public void read(long invertedFileLength, long vocabularyLength, RandomAccessFile input) throws IOException {
    long last = 0;
    long start = input.getFilePointer();

    while (input.getFilePointer() < start + vocabularyLength) {
      short length = input.readShort();
      byte[] data = new byte[length];
      input.read(data);
      long offset = input.readLong();
      TermSlot slot = new TermSlot();

      if (slots.size() > 0) {
        slots.get(slots.size() - 1).length = offset - last;
      }
      slot.begin = offset;
      slot.termData = data;
      slots.add(slot);

      last = offset;
    }

    if (slots.size() > 0) {
      slots.get(slots.size() - 1).length = invertedFileLength - last;
    }
    assert invertedFileLength >= last;
  }

  public TermSlot get(byte[] key) {
    if (slots.size() == 0) {
      return null;
    }
    int big = slots.size() - 1;
    int small = 0;

    while (big - small > 1) {
      int middle = small + (big - small) / 2;
      byte[] middleKey = slots.get(middle).termData;

      if (Utility.compare(middleKey, key) <= 0) {
        small = middle;
      } else {
        big = middle;
      }
    }

    TermSlot one = slots.get(small);
    TermSlot two = slots.get(big);

    if (Utility.compare(two.termData, key) <= 0) {
      return two;
    } else {
      return one;
    }
  }
}
