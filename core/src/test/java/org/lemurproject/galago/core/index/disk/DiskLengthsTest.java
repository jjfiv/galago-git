/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DiskLengthsReader.KeyIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskLengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 *
 * @author sjh
 */
public class DiskLengthsTest{

  @Test
  public void testLengths() throws IOException {
    File len = null;
    try {

      len = FileUtility.createTemporary();

      Parameters p = Parameters.instance();
      p.set("filename", len.getAbsolutePath());
      DiskLengthsWriter writer = new DiskLengthsWriter(new FakeParameters(p));

      byte[] key = ByteUtil.fromString("document");
      for (int i = 10; i <= 100; i++) {
        writer.process(new FieldLengthData(key, i, i + 1));
      }

      writer.process(new FieldLengthData(key, 110, 111));

      writer.close();

      DiskLengthsReader reader = new DiskLengthsReader(len.getAbsolutePath());

      // first some random seeks
      assertEquals(reader.getLength(90), 91);
      assertEquals(reader.getLength(50), 51);
      assertEquals(reader.getLength(105), 0);
      assertEquals(reader.getLength(110), 111);

      KeyIterator ki = reader.getIterator();
      DiskLengthsIterator streamItr = ki.getStreamValueIterator();

      ScoringContext sc = new ScoringContext();

      streamItr.syncTo(50);
      sc.document = 50;
      assertEquals(streamItr.currentCandidate(), 50);
      assertEquals(streamItr.length(sc), 51);

      streamItr.syncTo(90);
      sc.document = 90;
      assertEquals(streamItr.currentCandidate(), 90);
      assertEquals(streamItr.length(sc), 91);

      streamItr.syncTo(90);
      sc.document = 90;
      assertEquals(streamItr.currentCandidate(), 90);
      assertEquals(streamItr.length(sc), 91);

      streamItr.syncTo(110);
      sc.document = 110;
      assertEquals(streamItr.currentCandidate(), 110);
      assertEquals(streamItr.length(sc), 111);

      streamItr.syncTo(200);
      sc.document = 200;
      assertEquals(streamItr.currentCandidate(), 110);
      assertEquals(streamItr.length(sc), 0);

      reader.close();

    } finally {
      if (len != null) {
        assertTrue(len.delete());
      }

    }
  }

  @Test
  public void testBigLengths() throws IOException {
    File len = FileUtility.createTemporary();
    try {

      Parameters p = Parameters.instance();
      p.set("filename", len.getAbsolutePath());
      DiskLengthsWriter writer = new DiskLengthsWriter(new FakeParameters(p));

      byte[] key = ByteUtil.fromString("document");

      long min = 8000000000L;
      long max = 8000100000L;
      long step = 1000L;
      int j = 1;
      for (long i = min; i <= max; i += step) {
        writer.process(new FieldLengthData(key, i, j));
        j += 1;
      }

      writer.close();

      DiskLengthsReader reader = new DiskLengthsReader(len.getAbsolutePath());

      DiskLengthSource ls = reader.getLengthsSource();
      int l = 1;
      while(!ls.isDone()){
        assertEquals(ls.length(ls.currentCandidate()), l);
        l += 1;
        ls.syncTo(ls.currentCandidate() + step);
      }
      
      reader.close();

    } finally {
      assertTrue(len.delete());
    }
  }
}
