/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.VocabularyWriter;
import org.lemurproject.galago.utility.compression.VByte;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor
 */
public class VocabularyWriterTest {
  @Test
  public void testWriter() throws Exception {
    VocabularyWriter writer = new VocabularyWriter();

    byte[] first = "first".getBytes("UTF-8");
    byte[] second = "second".getBytes("UTF-8");
    writer.add(first, 0, (short) 10);
    writer.add(second, 256, (short) 10);

    byte[] output = writer.data();
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(output));

    byte[] buffer = new byte[5];
    assertEquals(5, VByte.uncompressInt(input));
    input.read(buffer);
    assertTrue(Arrays.equals(first, buffer));
    assertEquals(0, VByte.uncompressLong(input));
    assertEquals(10, VByte.uncompressInt(input));

    buffer = new byte[6];
    assertEquals(6, VByte.uncompressInt(input));
    input.read(buffer);
    assertTrue(Arrays.equals(second, buffer));
    assertEquals(256, VByte.uncompressLong(input));
    assertEquals(10, VByte.uncompressInt(input));

  }
}
