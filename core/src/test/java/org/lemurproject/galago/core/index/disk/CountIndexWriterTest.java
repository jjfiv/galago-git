/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.CountIndexReader.KeyIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskCountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class CountIndexWriterTest extends TestCase {

  public CountIndexWriterTest(String testName) {
    super(testName);
  }

  public void testCountIndex() throws Exception {
    File tmp = Utility.createTemporary();
    try {
      Parameters p = new Parameters();
      p.set("filename", tmp.getAbsolutePath());
      CountIndexWriter writer = new CountIndexWriter(new FakeParameters(p));

      int c = 1;

      // NORMAL TEST:
      writer.processWord(Utility.fromString("test1"));
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
      writer.processWord(Utility.fromString("test2"));
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

    } finally {
      tmp.delete();
    }
  }
}