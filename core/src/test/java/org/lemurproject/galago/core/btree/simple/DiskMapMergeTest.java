// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.btree.simple;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.ByteUtil;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import static org.junit.Assert.*;

/**
 *
 * @author jfoley
 */
public class DiskMapMergeTest {
	@Test
  public void testMerge() throws IOException {
    byte[] foo = ByteUtil.fromString("foo");
    byte[] bar = ByteUtil.fromString("bar");
    byte[] baz = ByteUtil.fromString("baz");
    byte[] hmm = ByteUtil.fromString("hmm");

    HashMap<byte[], byte[]> dataA = new HashMap<byte[], byte[]>();
    dataA.put(foo, bar);
    dataA.put(bar, baz);
    HashMap<byte[], byte[]> dataB = new HashMap<byte[], byte[]>();
    dataB.put(baz, hmm);
    dataB.put(hmm, foo);
    
    HashMap<byte[], byte[]> data = new HashMap<byte[], byte[]>();
    data.put(foo, bar);
    data.put(bar, baz);
    data.put(baz, hmm);
    data.put(hmm, foo);
    
    File tmpA = FileUtility.createTemporary();
    tmpA.deleteOnExit();
    File tmpB = FileUtility.createTemporary();
    tmpB.deleteOnExit();

    /**
     * Build an on-disk maps using galago
     */
    DiskMapReader.fromMap(tmpA.getAbsolutePath(), dataA);
    DiskMapReader.fromMap(tmpB.getAbsolutePath(), dataB);
    
    File tmpC = FileUtility.createTemporary();
    tmpC.deleteOnExit();
    
    DiskMapReader onDisk = DiskMapMerger.merge(tmpC.getAbsolutePath(), Arrays.asList(tmpA.getAbsolutePath(), tmpB.getAbsolutePath()));

    /*
     * pull keys
     */
    Set<byte[]> diskKeys = onDisk.keySet();
    Set<byte[]> memKeys = data.keySet();

    assertEquals(diskKeys.size(), memKeys.size());

    // java using object equality is dumb
    // assertTrue(memKeys.contains(key)) will fail because it does pointer comparisons...
    for (byte[] key : memKeys) {
      assertTrue(onDisk.containsKey(key));
      assertArrayEquals((byte[]) onDisk.get(key), (byte[]) data.get(key));
    }
  }
}
