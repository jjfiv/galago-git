// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.tupleflow;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author trevor
 */
public abstract class DataStream extends InputStream implements DataInput {
    public abstract DataStream subStream(long start, long length);
    public abstract long getPosition();
    public abstract boolean isDone();
    public abstract long length();

    @Override
    public int read() throws IOException {
      if(isDone()) { return -1; }
      return readUnsignedByte();
    }

    /**
     * Seeks forward into the stream to a particular byte offset (reverse
     * seeks are not allowed).  The offset is relative to the start position of
     * this data stream, not the beginning of the file.
     */
    public abstract void seek(long offset);
}
