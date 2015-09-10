// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.utility.buffer;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author trevor
 */
public abstract class DataStream extends InputStream implements DataInput {
    public abstract DataStream subStream(long start, long length) throws IOException;
    public abstract long getPosition();
    public abstract boolean isDone();
    public abstract long length();

    @Override
    public int read() throws IOException {
      if(isDone()) { return -1; }
      return readUnsignedByte();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read;
        for(read=0; read<len && !isDone(); read++) {
            b[off+read] = (byte) readUnsignedByte();
        }
        return read;
    }

    /**
     * Seeks forward into the stream to a particular byte offset (reverse
     * seeks are not allowed).  The offset is relative to the start position of
     * this data stream, not the beginning of the file.
     */
    public abstract void seek(long offset);
}
