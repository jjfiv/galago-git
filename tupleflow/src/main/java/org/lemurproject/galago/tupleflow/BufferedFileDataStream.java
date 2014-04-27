    // BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

    import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;

    /**
     *
     * @author trevor
     */
public class BufferedFileDataStream extends DataStream {

  // the fileStream object must be used in a synchronous manner
  // as this class is used heavily by the IndexReader class
  final RandomAccessFile fileStream;
  long stopPosition;
  long startPosition;
  final static int cacheLength = 32768;
  long bufferStart;
  int bufferPosition;
  byte[] cacheBuffer;

  /** Creates a new instance of BufferedFileDataStream */
  public BufferedFileDataStream(RandomAccessFile input, long start, long end) {
    assert start <= end;

    this.fileStream = input;
    this.stopPosition = end;
    this.cacheBuffer = new byte[0];
    this.bufferPosition = 0;
    this.bufferStart = start;
    this.startPosition = start;
  }

  @Override
  public BufferedFileDataStream subStream(long start, long length) {
    assert start < length();
    assert start + length <= length();
    return new BufferedFileDataStream(
            fileStream, bufferStart + start,
            bufferStart + start + length);
  }

  @Override
  public boolean isDone() {
    final long position = bufferStart + bufferPosition;
    return position >= stopPosition;
  }

  @Override
  public long length() {
    return stopPosition - startPosition;
  }

  @Override
  public long getPosition() {
    return getAbsolutePosition() - startPosition;
  }

  public long getAbsolutePosition() {
    return bufferStart + bufferPosition;
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
   * Seeks forward into the fileStream to a particular byte offset (reverse
   * seeks are not allowed).  The offset is relative to the start of the file.
   */
  private void seekAbsolute(long offset) {
    assert bufferStart + bufferPosition <= offset;

    // is any of this data cached?
    if (offset - bufferStart < cacheBuffer.length) {
      // this cast is safe because we know it's smaller than cacheBuffer.length
      bufferPosition = (int) (offset - bufferStart);
    } else {
      // this sets the fileStream position to the appropriate point,
      // and effectively invalidates the current cache contents.
      bufferStart = offset - cacheBuffer.length;
      bufferPosition = cacheBuffer.length;
    }
  }

  @Override
  public void readFully(byte[] buffer, int start, int length) throws IOException {
    cache(length);
    System.arraycopy(cacheBuffer, bufferPosition, buffer, start, length);
    update(length);
  }

  @Override
  public void readFully(byte[] buffer) throws IOException {
    cache(buffer.length);
    System.arraycopy(cacheBuffer, bufferPosition, buffer, 0, buffer.length);
    update(buffer.length);
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

    int a = cacheByte(0);
    int b = cacheByte(1);
    int c = cacheByte(2);
    int d = cacheByte(3);

    int result = ((a & 0xff) << 24)
            | ((b & 0xff) << 16)
            | ((c & 0xff) << 8)
            | (d & 0xff);

    update(4);
    return result;
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
    throw new IOException("readUTF is unimplemented");
  }

  // inlining here for performance
  @Override
  public int readUnsignedByte() throws IOException {
    if (cacheBuffer.length - bufferPosition >= 1) {
      int result = 0xff & (int) cacheBuffer[bufferPosition];
      bufferPosition += 1;
      return result;
    } else {
      cache(1);
      int b = cacheByte(0);
      update(1);
      return b & 0xff;
    }
  }

  private void cache(int length) throws IOException {
    assert length >= 0 : "Length can't be negative: " + length + " "
            + bufferStart + " " + bufferPosition + " " + stopPosition;

    // quick check to see if it's already buffered
    if (cacheBuffer.length - bufferPosition >= length) {
      return;        // if it's not buffered, is there enough room left in the
      // file to cache this much data?
    }
    if (bufferStart + bufferPosition + length > stopPosition) {
      throw new EOFException("Tried to read off the end of the file.\n"+
        "bufferStart: "+bufferStart+" bufferPos: "+bufferPosition+" length:"+length+" stopAt:"+stopPosition);
    }
    long current = bufferStart + bufferPosition;
    int readLength = (int) Math.min(stopPosition - current, cacheLength);
    readLength = Math.max(readLength, length);

    if (readLength != cacheBuffer.length) {
      cacheBuffer = new byte[readLength];
    }

    // this is the only time this file accesses the fileStream
    synchronized (fileStream) {
      fileStream.seek(current);
      fileStream.readFully(cacheBuffer);
    }

    bufferStart = current;
    bufferPosition = 0;
  }

  private void update(int length) {
    bufferPosition += length;
  }

  private byte cacheByte(int i) {
    return cacheBuffer[bufferPosition + i];
  }

}
