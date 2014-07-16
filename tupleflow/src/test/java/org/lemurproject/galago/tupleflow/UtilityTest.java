/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.tupleflow;

import org.junit.Assert;
import org.junit.Test;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.StreamUtil;
import org.lemurproject.galago.utility.compression.VByte;

import java.io.*;

import static org.junit.Assert.*;

/**
 *
 * @author trevor
 */
public class UtilityTest {

  @Test
  public void testCopyStream() throws IOException {
    byte[] data = {0, 1, 2, 3, 4, 5};
    ByteArrayInputStream input = new ByteArrayInputStream(data);
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    StreamUtil.copyStream(input, output);
    byte[] result = output.toByteArray();
    assertEquals(0, CmpUtil.compare(data, result));
    assertTrue(CmpUtil.equals(data, result));
  }

  @Test
  public void testMakeParentDirectories() throws IOException {
    // This gives us a usable temporary path.
    File f = null;
    File bbb = null;
    try {
      f = FileUtility.createTemporary();
      bbb = new File(f.getParent(), "bbb");

      String parent = f.getParent() + File.separator
              + Utility.join(new String[]{"bbb", "b", "c", "d"}, File.separator);
      String path = parent + File.separator + "e";
      FSUtil.makeParentDirectories(path);

      // The parent directory should exist
      assertTrue(new File(parent).isDirectory());
      // but the file itself should not exist.
      assertFalse(new File(path).exists());

    } finally {
      if (f != null) {
        Assert.assertTrue(f.delete());
      } if (bbb != null) {
        FSUtil.deleteDirectory(bbb);
      }
    }
  }

  @Test
  public void testConverters() throws IOException {
    // String
    String testString = "I am a little teapot, short and stout";
    byte[] buffer = ByteUtil.fromString(testString);
    String convString = ByteUtil.toString(buffer);
    assertTrue(testString.equals(convString));

    // short
    short testShort = 2345;
    buffer = Utility.fromShort(testShort);
    short convShort = Utility.toShort(buffer);
    assertEquals(testShort, convShort);

    // int
    int testInt = 832905257;
    buffer = Utility.fromInt(testInt);
    int convInt = Utility.toInt(buffer);
    assertEquals(testInt, convInt);

    // long
    long testLong = 9034790013458L;
    buffer = Utility.fromLong(testLong);
    long convLong = Utility.toLong(buffer);
    assertEquals(testLong, convLong);

    // compression
    DataOutputStream out;
    ByteArrayOutputStream bout;
    DataInputStream in;
    ByteArrayInputStream bin;
    bout = new ByteArrayOutputStream();
    out = new DataOutputStream(bout);

    // int on data streams
    testInt = 313;
    VByte.compressInt(out, testInt);
    bin = new ByteArrayInputStream(bout.toByteArray());
    in = new DataInputStream(bin);
    convInt = VByte.uncompressInt(in);
    assertEquals(testInt, convInt);

    // int uncompressed from byte array
    buffer = bout.toByteArray();
    convInt = VByte.uncompressInt(buffer, 0);
    assertEquals(testInt, convInt);

    // long on data streams
    bout.reset();
    testLong = 453279;
    VByte.compressLong(out, testLong);
    bin = new ByteArrayInputStream(bout.toByteArray());
    in = new DataInputStream(bin);
    convLong = VByte.uncompressLong(in);
    assertEquals(testLong, convLong);

    // long uncompressed from byte array
    buffer = bout.toByteArray();
    convLong = VByte.uncompressLong(buffer, 0);
    assertEquals(testLong, convLong);
  }
}
