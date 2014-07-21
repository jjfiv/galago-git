package org.lemurproject.galago.core.index.disk;

import org.junit.Test;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.CmpUtil;

import static org.junit.Assert.assertTrue;

public class DiskBTreeWriterTest {

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