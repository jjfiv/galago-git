/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.SparseFloatListReader.KeyIterator;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class SparseFloatListWriterTest {
  @Test
  public void testSomeMethod() throws Exception {
    File tmp = FileUtility.createTemporary();
    try {
      Parameters p = Parameters.instance();
      p.set("filename", tmp.getAbsolutePath());
      SparseFloatListWriter writer = new SparseFloatListWriter(new FakeParameters(p));

      int c = 1;

      // NORMAL TEST:
      writer.processWord(Utility.fromString("test1"));
      for (long doc = 0; doc < 2020; doc += 2) {
        writer.processNumber(doc);
        writer.processTuple(c + 0.1);
        c += 1;
      }

      // VERY LARGE COLLECTION TEST:
      long min = 2000000000L;
      long max = 8000000000L;
      long step = (max - min) / 1010;
      c = 1;
      writer.processWord(Utility.fromString("test2"));
      for (long doc = min; doc < max; doc += step) {
        writer.processNumber(doc);
        writer.processTuple(c + 0.1);
        c += 1;
      }

      writer.close();

      SparseFloatListReader r = new SparseFloatListReader(tmp.getAbsolutePath());
      KeyIterator ki = r.getIterator();
      int keyCount = 0;
      while (!ki.isDone()) {
        keyCount += 1;

        SparseFloatListSource vs = ki.getValueSource();
        int expC = 1;
        while (!vs.isDone()) {
          assertEquals(vs.score(vs.currentCandidate()), expC + 0.1, 0.001);

          expC += 1;
          vs.movePast(vs.currentCandidate());
        }

        ki.nextKey();
      }
      assertEquals(keyCount, 2);

    } finally {
      tmp.delete();
    }
  }
}
