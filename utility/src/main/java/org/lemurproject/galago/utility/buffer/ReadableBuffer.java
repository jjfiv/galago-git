package org.lemurproject.galago.utility.buffer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This interface covers any thread-safe, readable buffer.
 * For more convenient access to the data, consider a class that uses it: BufferedFileDataStream.
 * @see CachedBufferDataStream
 * @author jfoley.
 */
public interface ReadableBuffer extends Closeable {
  /** Fill up a Byte Buffer starting at offset in this buffer. Returns the number of bytes read. */
  public int read(ByteBuffer buf, long offset) throws IOException;
  /** Return the total number of bytes in this buffer. */
  public long length() throws IOException;
}
