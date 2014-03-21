/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.VByteInput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author irmarc
 */
public class DiskSpillCompressedByteBufferTest {
  @Test
  public void testSpill() throws Exception {
    DiskSpillCompressedByteBuffer instance = new DiskSpillCompressedByteBuffer(1, 6, 24);
    for (int i = 0; i < 400; i++) {
      instance.addRaw(i);
    }

    assertEquals(instance.length(), 400L);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    instance.write(stream);
    byte[] result = stream.toByteArray();
    assertEquals(result.length, 400);
    for (int i = 0; i < 400; i++) {
      assertEquals(result[i], (byte) i);
    }

    // now delete the spill file
    instance.clear();
  }

  @Test
  public void testAdd() throws Exception {
    DiskSpillCompressedByteBuffer instance = new DiskSpillCompressedByteBuffer();
    instance.add(5);
    instance.add(10);
    instance.add(200);
    instance.add(400);

    byte[] result = new byte[]{(byte) (5 | 1 << 7),
        (byte) (10 | 1 << 7),
        72,
        (byte) (1 | 1 << 7),
        16,
        (byte) (3 | 1 << 7)};

    assertEquals(result.length, instance.length());
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    instance.write(stream);
    byte[] written = stream.toByteArray();
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], written[i]);
    }
  }

  @Test
  public void testAddRaw() throws Exception {
    CompressedByteBuffer instance = new CompressedByteBuffer();
    instance.addRaw(5);
    instance.addRaw(6);

    assertEquals(2, instance.length());
    assertTrue(instance.getBytes().length >= 2);
    assertEquals(5, instance.getBytes()[0]);
    assertEquals(6, instance.getBytes()[1]);
  }

  @Test
  public void testAddFloat() throws Exception {
    float f = 1.0F;
    DiskSpillCompressedByteBuffer instance = new DiskSpillCompressedByteBuffer();
    instance.addFloat(f);

    assertEquals(4, instance.length());
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    instance.write(stream);
    byte[] result = stream.toByteArray();
    int floatBits = Float.floatToIntBits(f);

    assertEquals(result[0], (byte) (floatBits >> 24));
    assertEquals(result[1], (byte) (floatBits >> 16));
    assertEquals(result[2], (byte) (floatBits >> 8));
    assertEquals(result[3], (byte) (floatBits));
  }

  @Test
  public void testSpecial() throws Exception {
    DiskSpillCompressedByteBuffer instance = new DiskSpillCompressedByteBuffer();
    final int[] numbers = {1, 2, 2, 1, 0, 8, 1, 0, 8, 5, 5, 0};

    for (int number : numbers) {
      instance.add(number);
    }

    assertEquals(numbers.length, instance.length());
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    instance.write(stream);
    byte[] written = stream.toByteArray();
    VByteInput input = new VByteInput(new DataInputStream(new ByteArrayInputStream(written)));
    // do it with read int
    for (int number : numbers) {
      int read = input.readInt();
      assertEquals(number, read);
    }

    // Now with longs
    input = new VByteInput(new DataInputStream(new ByteArrayInputStream(written)));
    // do it with read int
    for (int number : numbers) {
      long read = input.readLong();
      assertEquals((long) number, read);
    }
  }
}
