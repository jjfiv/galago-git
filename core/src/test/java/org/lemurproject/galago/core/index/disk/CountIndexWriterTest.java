/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.CountIndexReader.KeyIterator;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class CountIndexWriterTest {
  @Test
  public void testCountIndex() throws Exception {
    File tmp = FileUtility.createTemporary();
    try {
      Parameters p = Parameters.instance();
      p.set("filename", tmp.getAbsolutePath());
      CountIndexWriter writer = new CountIndexWriter(new FakeParameters(p));

      int c = 1;

      // NORMAL TEST:
      writer.processWord(ByteUtil.fromString("test1"));
      for (long doc = 0; doc < 2020; doc += 2) {
        writer.processDocument(doc);
        writer.processTuple(c);
        c += 1;
      }

      // VERY LARGE COLLECTION TEST:
      long min = 2000000000L;
      long max = 8000000000L;
      long step = (max - min) / 1010;
      c = 1;
      writer.processWord(ByteUtil.fromString("test2"));
      for (long doc = min; doc < max; doc += step) {
        writer.processDocument(doc);
        writer.processTuple(c);
        c += 1;
      }

      writer.close();

      CountIndexReader r = new CountIndexReader(tmp.getAbsolutePath());
      KeyIterator ki = r.getIterator();
      int keyCount = 0;
      while (!ki.isDone()) {
        keyCount += 1;

        CountIndexCountSource vs = ki.getStreamValueSource();
        int expC = 1;
        while (!vs.isDone()) {
          assertEquals(vs.count(vs.currentCandidate()), expC);
          expC += 1;
          vs.movePast(vs.currentCandidate());
        }

        ki.nextKey();
      }
      assertEquals(keyCount, 2);
      r.close();

    } finally {
      assertTrue(tmp.delete());
    }
  }
}
