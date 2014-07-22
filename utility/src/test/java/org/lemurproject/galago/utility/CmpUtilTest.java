package org.lemurproject.galago.utility;

import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

public class CmpUtilTest {

  @Test
  public void testUnicodeBug() {
    String bb = ByteUtil.toString(new byte[] {
      -16, -99, -124, -85
    });
    String allah = ByteUtil.toString(new byte[] {
      -17, -73, -78
    });

    //double-flat symbol
    byte[] bbB = ByteUtil.fromString(bb);
    // allah in arabic
    byte[] allahB = ByteUtil.fromString(allah);

    // string compare should NOT be equivalent to byte compare for certain utf8 things.
    assertNotEquals(CmpUtil.compare(bbB, allahB), CmpUtil.compare(bb, allah));
    assertNotEquals(CmpUtil.compare(allahB, bbB), CmpUtil.compare(allah, bb));
  }

}