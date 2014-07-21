package org.lemurproject.galago.utility;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class CmpUtilTest {

  @Test
  public void testUnicodeBug() {
    String firstBadName = "\uf48d\u81b7.html";
    byte[] firstBytes = ByteUtil.fromString(firstBadName);
    String secondBadName = ByteUtil.toString(new byte[]{-18, -128, -128, 0x2e, 0x68, 0x74, 0x6d, 0x6c});
    byte[] secondBytes = ByteUtil.fromString(secondBadName);

    int cmp1 = CmpUtil.compare(firstBadName, secondBadName);
    int cmp2 = CmpUtil.compare(firstBytes, secondBytes);

    assertTrue(cmp1 > 0);
    assertTrue(cmp2 > 0);
  }

}