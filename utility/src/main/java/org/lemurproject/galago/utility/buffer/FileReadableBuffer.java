package org.lemurproject.galago.utility.buffer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author jfoley.
 */
public class FileReadableBuffer implements ReadableBuffer {
  private final FileChannel channel;
  private final RandomAccessFile file;

  public FileReadableBuffer(RandomAccessFile raf) {
    this.channel = raf.getChannel();
    this.file = raf;
  }

  public FileReadableBuffer(String path) throws FileNotFoundException {
    this(new RandomAccessFile(path, "r"));
  }

  @Override
  public int read(ByteBuffer buf, long offset) throws IOException {
    return channel.read(buf, offset);
  }

  @Override
  public long length() throws IOException {
    return file.length();
  }

  @Override
  public void close() throws IOException {
    file.close();
  }
}
