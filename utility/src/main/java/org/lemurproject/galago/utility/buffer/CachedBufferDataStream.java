// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility.buffer;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * This is a forward-only view of a ReadableBuffer as a DataStream.
 * seek is *only* relative, etc.
 * @author trevor
 */
public class CachedBufferDataStream extends DataStream {
  final ReadableBuffer fileStream;
  ByteBuffer bbCache;
  long stopPosition;
  long startPosition;
  final static int cacheLength = 0x8000; //32768;
  long bufferStart;
  //byte[] cacheBuffer;

  /** Creates a new create of BufferedFileDataStream */
  public CachedBufferDataStream(RandomAccessFile input, long start, long end) {
    this(new FileReadableBuffer(input), start, end);
  }

  public CachedBufferDataStream(ReadableBuffer buffer, long start, long end) {
    assert start <= end;
    this.fileStream = buffer;
    this.stopPosition = end;
    this.bbCache = ByteBuffer.allocate(0);
    //this.cacheBuffer = new byte[0];
    this.bufferStart = start;
    this.startPosition = start;
  }

  public CachedBufferDataStream(RandomAccessFile randomAccessFile) throws IOException {
    this(new FileReadableBuffer(randomAccessFile));
  }

  public CachedBufferDataStream(ReadableBuffer buffer) throws IOException {
    this(buffer, 0, buffer.length());
  }

  @Override
  public CachedBufferDataStream subStream(long start, long length) throws IOException {
    assert start < length();
    assert start + length <= length();
    return new CachedBufferDataStream(
        fileStream, bufferStart + start,
        bufferStart + start + length);
  }

  @Override
  public boolean isDone() {
    return getAbsolutePosition() >= stopPosition;
  }

  @Override
  public long length() {
    return stopPosition - startPosition;
  }

  @Override
  public long getPosition() {
    return (bufferStart-startPosition)+bbCache.position();
  }

  public long getAbsolutePosition() {
    return bufferStart+bbCache.position();
  }

  /**
   * Seeks forward into the fileStream to a particular byte offset (reverse
   * seeks are not allowed).  The offset is relative to the start position of
   * this data fileStream, not the beginning of the file.
   */
  @Override
  public void seek(long offset) {
    seekAbsolute(offset + startPosition);
  }

  /**
   * Seeks forward into the fileStream to a particular byte offset.
   * The offset is relative to the start of the file.
   */
  private void seekAbsolute(long offset) {
    // Only allow forward-seeking.
    assert bufferStart + bbCache.position() <= offset;

    // is any of this data cached?
    // int length = cacheBuffer.length;
    int length = bbCache.limit();
    if(offset - bufferStart < length) {
      // this cast is safe because we know it's smaller than cacheBuffer.length
      bbCache.position((int) (offset - bufferStart));
    } else {
      // this sets the fileStream position to the appropriate point,
      // and effectively invalidates the current cache contents.
      bufferStart = offset - length;
      bbCache.position(length);
    }
  }

  @Override
  public void readFully(byte[] buffer, int start, int length) throws IOException {
    cache(length);
    bbCache.get(buffer, start, length);
  }

  @Override
  public void readFully(byte[] buffer) throws IOException {
    cache(buffer.length);
    bbCache.get(buffer);
  }

  @Override
  public int skipBytes(int n) throws IOException {
    update(n);
    return n;
  }

  @Override
  public int readUnsignedShort() throws IOException {
    cache(2);

    byte a = cacheByte(0);
    byte b = cacheByte(1);

    int result = (((a << 8) | (b & 0xff)) & 0xffff);

    update(2);
    return result;
  }

  @Override
  public boolean readBoolean() throws IOException {
    cache(1);
    boolean result = (cacheByte(0) != 0);
    update(1);
    return result;
  }

  @Override
  public byte readByte() throws IOException {
    cache(1);
    byte result = cacheByte(0);
    update(1);
    return result;
  }

  @Override
  public char readChar() throws IOException {
    return (char) readShort();
  }

  @Override
  public short readShort() throws IOException {
    cache(2);
    byte a = cacheByte(0);
    byte b = cacheByte(1);
    short result = (short) ((a << 8) | (b & 0xff));
    update(2);
    return result;
  }

  @Override
  public double readDouble() throws IOException {
    long result = readLong();
    return Double.longBitsToDouble(result);
  }

  @Override
  public float readFloat() throws IOException {
    int result = readInt();
    return Float.intBitsToFloat(result);
  }

  @Override
  public int readInt() throws IOException {
    cache(4);
    return bbCache.getInt();
  }

  @Override
  public String readLine() throws IOException {
    throw new IOException("readLine is unimplemented and deprecated");
  }

  @Override
  public long readLong() throws IOException {
    long a = readInt();
    long b = readInt();

    // shift a to high word
    a <<= 32;
    // mask b
    b &= (0xFFFFFFFFL);

    return a | b;
  }

  @Override
  public String readUTF() throws IOException {
    throw new UnsupportedOperationException("readUTF is unimplemented");
  }

  // inlining here for performance
  @Override
  public int readUnsignedByte() throws IOException {
    //int length = cacheBuffer.length;
    int length = bbCache.limit();
    if (length - bbCache.position() >= 1) {
      return 0xff & (int) bbCache.get();
    } else {
      cache(1);
      int b = cacheByte(0);
      update(1);
      return b & 0xff;
    }
  }

  private void cache(int length) throws IOException {
    assert length >= 0 : "Length can't be negative: " + length + " "
        + bufferStart + " " + stopPosition;

    // quick check to see if it's already buffered
    int bufLength = bbCache.limit();
    if (bufLength - bbCache.position() >= length) {
      return;
      // if it's not buffered, is there enough room left in the
      // file to cache this much data?
    }
    if (bufferStart + bbCache.position() + length > stopPosition) {
      throw new EOFException("Tried to read off the end of the file.\n"+
          "bufferStart: "+bufferStart+" bufferPos: "+bbCache.position()+" length:"+length+" stopAt:"+stopPosition);
    }

    long current = bufferStart + bbCache.position();
    int readLength = (int) Math.min(stopPosition - current, cacheLength);
    readLength = Math.max(readLength, length);

    if(readLength != bbCache.capacity()) {
      bbCache = ByteBuffer.allocate(readLength);
    }
    bbCache.rewind();
    int amountRead = fileStream.read(bbCache, current);
    /*
      System.out.println("#");
      System.out.println("readLength:"+readLength);
      System.out.println("capacity: "+bbCache.capacity());
      System.out.println("amountRead: "+amountRead);
      System.out.println("bufferStart: "+bufferStart);
      System.out.println("startPosition: "+startPosition);
      System.out.println("stopPosition: "+stopPosition);
      System.out.println("position: "+bbCache.position());
      System.out.println("current: "+current);
      System.out.println("length: "+length());
    */
    assert(amountRead >= readLength);
    bbCache.rewind();
    bufferStart = current;
  }

  private void update(int length) throws IOException {
    int nextPos = bbCache.position()+length;
    if(nextPos > bbCache.limit()) {
      bufferStart += nextPos;
      bbCache = ByteBuffer.allocate(0);
      return;
    }
    bbCache.position(nextPos);
  }

  private byte cacheByte(int i) {
    return bbCache.get(bbCache.position()+i);
  }

}
