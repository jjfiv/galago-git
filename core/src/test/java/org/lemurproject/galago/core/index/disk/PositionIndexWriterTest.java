/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.PositionIndexReader.KeyIterator;
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
public class PositionIndexWriterTest {

  @Test
  public void testCountIndex() throws Exception {
    File tmp = FileUtility.createTemporary();
    try {
      Parameters p = new Parameters();
      p.set("filename", tmp.getAbsolutePath());
      PositionIndexWriter writer = new PositionIndexWriter(new FakeParameters(p));
      
      int c = 1;

      // NORMAL TEST:
      writer.processWord(Utility.fromString("test1"));
      for (long doc = 0; doc < 2020; doc += 2) {
        writer.processDocument(doc);
        for (int pos = 0; pos < c; pos++) {
          writer.processPosition(pos);
          writer.processTuple();
        }
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
        for (int pos = 0; pos < c; pos++) {
          writer.processPosition(pos);
          writer.processTuple();
        }
        c += 1;
      }
      
      writer.close();
      
      PositionIndexReader r = new PositionIndexReader(tmp.getAbsolutePath());
      KeyIterator ki = r.getIterator();
      int keyCount = 0;
      while (!ki.isDone()) {
        keyCount += 1;
        
        PositionIndexExtentSource es = ki.getValueSource();
        int expC = 1;
        while (!es.isDone()) {
          assertEquals(es.count(es.currentCandidate()), expC);
          expC += 1;
          es.movePast(es.currentCandidate());
        }

        PositionIndexCountSource cs = ki.getValueCountSource();
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