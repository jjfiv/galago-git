/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.index.CompressedByteBuffer;
import org.lemurproject.galago.core.index.DiskSpillCompressedByteBuffer;
import java.io.ByteArrayInputStream;
import junit.framework.TestCase;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 *
 * @author irmarc
 */
public class CompressedRawByteBufferTest extends TestCase {

    public CompressedRawByteBufferTest(String testName) {
        super(testName);
    }

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

    public void testAddRaw() throws Exception {
        CompressedByteBuffer instance = new CompressedByteBuffer();
        instance.addRaw(5);
        instance.addRaw(6);

        assertEquals(2, instance.length());
        assertTrue(instance.getBytes().length >= 2);
        assertEquals(5, instance.getBytes()[0]);
        assertEquals(6, instance.getBytes()[1]);
    }

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
        assertEquals(result[3], (byte) (floatBits >> 0));
    }

    public void testSpecial() throws Exception {
        DiskSpillCompressedByteBuffer instance = new DiskSpillCompressedByteBuffer();
        int[] numbers = {1, 2, 2, 1, 0, 8, 1, 0, 8, 5, 5, 0};

        for (int i = 0; i < numbers.length; i++) {
            instance.add(numbers[i]);
        }

        assertEquals(numbers.length, instance.length());
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        instance.write(stream);
        byte[] written = stream.toByteArray();
        VByteInput input = new VByteInput(new DataInputStream(new ByteArrayInputStream(written)));
        // do it with read int
        for (int i = 0; i < numbers.length; i++) {
            int read = input.readInt();
            assertEquals(numbers[i], read);
        }

        // Now with longs
        input = new VByteInput(new DataInputStream(new ByteArrayInputStream(written)));
        // do it with read int
        for (int i = 0; i < numbers.length; i++) {
            long read = input.readLong();
            assertEquals((long) numbers[i], read);
        }
    }
}
