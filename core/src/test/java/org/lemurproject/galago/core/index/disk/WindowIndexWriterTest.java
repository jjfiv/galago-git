/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class WindowIndexWriterTest {
  @Test
  public void testSomeMethod() throws Exception {
    File tmp = FileUtility.createTemporary();
    try {
      Parameters p = new Parameters();
      p.set("filename", tmp.getAbsolutePath());
      WindowIndexWriter writer = new WindowIndexWriter(new FakeParameters(p));

      int c = 1;

      // NORMAL TEST:
      writer.processExtentName(Utility.fromString("test1"));
      for (long doc = 0; doc < 2020; doc += 2) {
        writer.processNumber(doc);
        for (int pos = 0; pos < c; pos++) {
          writer.processBegin(pos);
          writer.processTuple(pos + 1);
        }
        c += 1;
      }

      // VERY LARGE COLLECTION TEST:
      long min = 2000000000L;
      long max = 8000000000L;
      long step = (max - min) / 1010;
      c = 1;
      writer.processExtentName(Utility.fromString("test2"));
      for (long doc = min; doc < max; doc += step) {
        writer.processNumber(doc);
        for (int pos = 0; pos < c; pos++) {
          writer.processBegin(pos);
          writer.processTuple(pos + 1);
        }
        c += 1;
      }

      writer.close();

      WindowIndexReader r = new WindowIndexReader(tmp.getAbsolutePath());
      WindowIndexReader.KeyIterator ki = r.getIterator();
      int keyCount = 0;
      while (!ki.isDone()) {
        keyCount += 1;

        WindowIndexCountSource vs = ki.getCountValueSource();
        int expC = 1;
        while (!vs.isDone()) {
          assertEquals(vs.count(vs.currentCandidate()), expC);
          expC += 1;
          vs.movePast(vs.currentCandidate());
        }

        WindowIndexExtentSource cs = ki.getValueSource();
        expC = 1;
        while (!cs.isDone()) {
          assertEquals(cs.count(cs.currentCandidate()), expC);
          expC += 1;
          cs.movePast(cs.currentCandidate());
        }

        ki.nextKey();
      }
      assertEquals(keyCount, 2);

    } finally {
      tmp.delete();
    }
  }
}
