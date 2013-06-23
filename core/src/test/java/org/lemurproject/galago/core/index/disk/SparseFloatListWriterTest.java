/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import java.io.File;
import static junit.framework.Assert.assertEquals;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.SparseFloatListReader.KeyIterator;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class SparseFloatListWriterTest extends TestCase {
  
  public SparseFloatListWriterTest(String testName) {
    super(testName);
  }

  public void testSomeMethod() throws Exception {
    File tmp = Utility.createTemporary();
    try {
      Parameters p = new Parameters();
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
