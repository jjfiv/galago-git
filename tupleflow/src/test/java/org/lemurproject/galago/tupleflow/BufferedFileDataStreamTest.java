package org.lemurproject.galago.tupleflow;

import org.junit.Test;
import org.lemurproject.galago.utility.ByteUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BufferedFileDataStreamTest {

  @Test
  public void directlyTestThisDumbClass() throws IOException {
    File tmp = File.createTempFile("foo", ".bin");
    Utility.copyStringToFile("abcdefgh", tmp);
    BufferedFileDataStream bfds = new BufferedFileDataStream(new RandomAccessFile(tmp, "r"), 0, 8);
    assertEquals(0, bfds.getPosition());
    assertEquals(8, bfds.length());
    assertEquals(('a' << 8) | 'b', bfds.readChar());
    assertEquals(('c' << 8) | 'd', bfds.readChar());
    assertEquals(('e' << 8) | 'f', bfds.readChar());
    assertEquals(('g' << 8) | 'h', bfds.readChar());

    assertEquals(8, bfds.getPosition());
    assertEquals(8, bfds.bbCache.position());
    assertEquals(8, bfds.bbCache.limit());
    assertEquals(0, bfds.bufferStart);
    assertTrue(bfds.isDone());
    tmp.delete();
  }

  @Test
  public void directlyTestReadUnsignedByte() throws IOException {
    File tmp = File.createTempFile("foo", ".bin");
    Utility.copyStringToFile("abcdefgh", tmp);
    BufferedFileDataStream bfds = new BufferedFileDataStream(new RandomAccessFile(tmp, "r"), 3, 8);
    assertEquals(0, bfds.getPosition());
    assertEquals('d', bfds.readUnsignedByte());
    assertEquals('e', bfds.readUnsignedByte());
    byte[] rest = new byte[3];
    bfds.read(rest);
    assertEquals("fgh", ByteUtil.toString(rest));

    tmp.delete();
  }
}
