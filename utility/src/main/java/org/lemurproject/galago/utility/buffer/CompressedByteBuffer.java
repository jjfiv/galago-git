// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility.buffer;

import gnu.trove.list.array.TByteArrayList;
import gnu.trove.procedure.TByteProcedure;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Stores lists of integers in vbyte compressed form.  This
 * is useful for buffering data that will be stored
 * compressed on disk.
 * 
 * (12/03/2010, irmarc): Switched the ArrayList of boxed bytes to TByteArrayList from trove. LHF.
 * (12/19/2014, jfoley): Move copier instances to local functions to make this class threadsafe.
 *
 * @author trevor + modified by sjh
 * @author irmarc
 * 
 */
public class CompressedByteBuffer {

  public TByteArrayList values;

  public CompressedByteBuffer() {
    clear();
  }

  public CompressedByteBuffer(int resetSize) {
    clear();
    values.ensureCapacity(resetSize);
  }

  /**
   * Add a single byte to the buffer.  This byte is written
   * directly to the buffer without compression.
   *
   * @param value The byte value to add.
   */
  public void addRaw(int value) {
    values.add((byte) value);
  }

  /**
   * Adds a single number to the buffer.  This number is
   * converted to compressed form before it is stored.
   */
  public void add(long i) {
    if (i < 1 << 7) {
      addRaw((int) (i | 0x80));
    } else if (i < 1 << 14) {
      addRaw((int) (i >> 0) & 0x7f);
      addRaw((int) ((i >> 7) & 0x7f) | 0x80);
    } else if (i < 1 << 21) {
      addRaw((int) (i >> 0) & 0x7f);
      addRaw((int) (i >> 7) & 0x7f);
      addRaw((int) ((i >> 14) & 0x7f) | 0x80);
    } else {
      while (i >= 1 << 7) {
        addRaw((int) (i & 0x7f));
        i >>= 7;
      }

      addRaw((int) (i | 0x80));
    }
  }

  /**
   * Adds a floating point value, (4 bytes) to the buffer.
   * This is an uncompressed value.
   */
  public void addFloat(float value) {
    int bits = Float.floatToIntBits(value);

    addRaw((bits >>> 24) & 0xFF);
    addRaw((bits >>> 16) & 0xFF);
    addRaw((bits >>> 8) & 0xFF);
    addRaw(bits & 0xFF);
  }

  /**
   * Adds a double floating point value, (4 bytes) to the buffer.
   * This is an uncompressed value.
   */
  public void addDouble(double value) {
    long bits = Double.doubleToLongBits(value);

    addRaw((int) ((bits >>> 56) & 0xFF));
    addRaw((int) ((bits >>> 48) & 0xFF));
    addRaw((int) ((bits >>> 40) & 0xFF));
    addRaw((int) ((bits >>> 32) & 0xFF));
    addRaw((int) ((bits >>> 24) & 0xFF));
    addRaw((int) ((bits >>> 16) & 0xFF));
    addRaw((int) ((bits >>> 8) & 0xFF));
    addRaw((int) (bits & 0xFF));
  }
  
  
  /**
   * Copies the entire contents of another compressed
   * buffer to the end of this one.
   *
   * @param other The buffer to copy.
   */
  public void add(CompressedByteBuffer other) {
    ByteCopierProcedure copier = new ByteCopierProcedure();
    copier.target = this;
    other.values.forEach(copier);
    copier.target = null; // no danglers
  }

  /**
   * Erases the contents of this buffer and sets its
   * length to zero.
   */
  public void clear() {
    values = new TByteArrayList();
  }

  /**
   * Returns a byte array containing the contents of this buffer.
   * The array returned may be larger than the actual length of
   * the stored data.  Use the length method to determine the
   * true data length.
   */
  public byte[] getBytes() {
    return values.toArray();
  }

  /**
   * Returns the length of the data stored in this buffer.
   */
  public long length() {
    return values.size();
  }

  /**
   * Writes the contents of this buffer to a stream.
   */
  public void write(OutputStream stream) throws IOException {
    ByteWriterProcedure writer = new ByteWriterProcedure();
    writer.stream = stream;
    values.forEach(writer);
    writer.stream = null; //  don't want a dangling reference
  }

  private static class ByteWriterProcedure implements TByteProcedure {

    public OutputStream stream;

    public ByteWriterProcedure() {
    }

    @Override
    public boolean execute(byte value) {
      try {
        stream.write(value);
        return true;
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  private static class ByteCopierProcedure implements TByteProcedure {

    public CompressedByteBuffer target;

    public ByteCopierProcedure() {
    }

    @Override
    public boolean execute(byte value) {
      target.values.add(value);
      return true;
    }
  }
}
