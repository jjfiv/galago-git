// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.btree.simple;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.*;

/**
 *
 * @author jfoley
 */
public class DiskMapTest {
	@Test
  public void createMap() throws IOException {
    byte[] foo = Utility.fromString("foo space");
    byte[] bar = Utility.fromString("bar");
    byte[] baz = Utility.fromString("baz");
    byte[] hmm = Utility.fromString("hmm");

    Map<byte[], byte[]> data = new TreeMap<byte[], byte[]>(new Utility.ByteArrComparator());
    data.put(foo, bar);
    data.put(bar, baz);
    data.put(baz, hmm);
    data.put(hmm, foo);

    File tmp = FileUtility.createTemporary();
    tmp.deleteOnExit();

    /**
     * Build an on-disk map using galago
     */
    DiskMapReader onDisk = DiskMapReader.fromMap(tmp.getAbsolutePath(), data);

    /*
     * pull keys
     */
    Set<byte[]> diskKeys = onDisk.keySet();
    Set<byte[]> memKeys = data.keySet();

    assertEquals(diskKeys.size(), memKeys.size());

    for (byte[] key : memKeys) {
      assertTrue(memKeys.contains(key));
      assertTrue(diskKeys.contains(key));
      assertTrue(onDisk.containsKey(key));
      assertArrayEquals((byte[]) onDisk.get(key), (byte[]) data.get(key));
    }

    assertFalse(onDisk.containsKey(Utility.fromString("Not in the map!")));
    assertNull(onDisk.get(Utility.fromString("Still not in the map!")));

    // test that different address byte arrays still work
    assertTrue(onDisk.containsKey(Utility.fromString("foo space")));
    assertNotNull(onDisk.get(Utility.fromString("foo space")));
  }
	
}
