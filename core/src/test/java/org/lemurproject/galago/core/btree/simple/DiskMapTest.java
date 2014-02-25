// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.btree.simple;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author jfoley
 */
@RunWith(JUnit4.class)
public class DiskMapTest {
	@Test
  public void createMap() throws IOException {
    byte[] foo = Utility.fromString("foo");
    byte[] bar = Utility.fromString("bar");
    byte[] baz = Utility.fromString("baz");
    byte[] hmm = Utility.fromString("hmm");

    HashMap<byte[], byte[]> data = new HashMap<byte[], byte[]>();
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

    // java using object equality is dumb
    // assertTrue(memKeys.contains(key)) will fail because it does pointer comparisons...
    for (byte[] key : memKeys) {
      assertTrue(onDisk.containsKey(key));
      assertArrayEquals((byte[]) onDisk.get(key), (byte[]) data.get(key));
    }
  }
	
}
