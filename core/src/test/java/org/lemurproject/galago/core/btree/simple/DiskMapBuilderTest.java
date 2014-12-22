package org.lemurproject.galago.core.btree.simple;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.FSUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;

/**
 * @author jfoley.
 */
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

  @Test
  public void canLiveInAnIndex() throws Exception {
    File tmpDir = FileUtility.createTemporaryDirectory();
    try {
      // build an index
      StringBuilder trecCorpus = new StringBuilder();
      for(int i=0; i<10; i++) {
        trecCorpus.append(AppTest.trecDocument("doc" + i, "doc" + i + " text"));
      }
      File inputFile = new File(tmpDir, "input.trectext");
      File indexPath = new File(tmpDir, "index");
      Utility.copyStringToFile(trecCorpus.toString(), inputFile);

      App.main(new String[]{"build",
          "--stemmedPostings=false",
          "--corpus=false",
          "--indexPath=" + indexPath.getAbsolutePath(),
          "--inputPath=" + inputFile.getAbsolutePath()});

      // stick a map in the middle
      byte[] foo = ByteUtil.fromString("foo space");
      byte[] bar = ByteUtil.fromString("bar");
      byte[] baz = ByteUtil.fromString("baz");
      byte[] hmm = ByteUtil.fromString("hmm");

      Map<byte[], byte[]> data = new TreeMap<>(new CmpUtil.ByteArrComparator());
      data.put(foo, bar);
      data.put(bar, baz);
      data.put(baz, hmm);
      data.put(hmm, foo);

      File tmp = FileUtility.createTemporary();
      tmp.deleteOnExit();

      DiskMapReader onDisk = DiskMapReader.fromMap((new File(indexPath, "myCustomFeature")).getAbsolutePath(), data);

      // make sure map works
      assertTrue(onDisk.containsKey(ByteUtil.fromString("foo space")));
      assertTrue(onDisk.containsKey(ByteUtil.fromString("bar")));
      assertTrue(onDisk.containsKey(ByteUtil.fromString("baz")));
      assertTrue(onDisk.containsKey(ByteUtil.fromString("hmm")));

      // make sure retrieval works
      LocalRetrieval ret = new LocalRetrieval(new DiskIndex(indexPath.getAbsolutePath()));
      assertNotNull(ret);
    } finally {
      FSUtil.deleteDirectory(tmpDir);
    }

  }
}
