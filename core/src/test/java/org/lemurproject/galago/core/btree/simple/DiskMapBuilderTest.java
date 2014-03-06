package org.lemurproject.galago.core.btree.simple;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author jfoley.
 */
@RunWith(JUnit4.class)
public class DiskMapBuilderTest {

  @Test
  public void testBuilding() throws IOException {
    int N = 1000;

    File tmpFromMap = FileUtility.createTemporary();
    tmpFromMap.deleteOnExit();
    File tmpFromShuffled = FileUtility.createTemporary();
    tmpFromShuffled.deleteOnExit();

    // build up sorted map
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    DiskMapSortedBuilder sortedBuilder = new DiskMapSortedBuilder(tmpFromMap.getAbsolutePath());
    for(int i=0; i<N; i++) {
      byte[] kv = Utility.fromLong(i);
      keys.add(kv);
      sortedBuilder.put(kv, kv);
    }
    sortedBuilder.close();

    // build a similar map from unsorted data:
    DiskMapBuilder mapBuilder = new DiskMapBuilder(tmpFromShuffled.getAbsolutePath());
    // unsort the data
    ArrayList<byte[]> shuffled = new ArrayList<byte[]>(keys);
    Collections.shuffle(shuffled);

    for(byte[] pt : shuffled) {
      mapBuilder.put(pt, pt);
    }
    mapBuilder.close();

    DiskMapReader fromMap = new DiskMapReader(tmpFromMap.getAbsolutePath());
    DiskMapReader fromShuffled = new DiskMapReader(tmpFromShuffled.getAbsolutePath());

    assertEquals(N, fromMap.size());
    assertEquals(N, fromShuffled.size());
    for(int i=0; i<N; i++) {
      byte[] pt = Utility.fromLong(i);
      assertArrayEquals(pt, fromMap.get(pt));
      assertArrayEquals(pt, fromShuffled.get(pt));
      assertNull(fromShuffled.get(Utility.fromLong(i + N)));
      assertNull(fromMap.get(Utility.fromLong(i+N)));
    }
  }
}
