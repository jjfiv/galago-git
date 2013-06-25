/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.retrieval.iterator.disk.DiskLengthsIterator;
import java.io.File;
import java.io.IOException;
import static junit.framework.Assert.assertEquals;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.DiskLengthsReader.KeyIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DiskLengthsTest extends TestCase {

  public DiskLengthsTest(String name) {
    super(name);
  }

  public void testLengths() throws IOException {
    File len = null;
    try {

      len = Utility.createTemporary();

      Parameters p = new Parameters();
      p.set("filename", len.getAbsolutePath());
      DiskLengthsWriter writer = new DiskLengthsWriter(new FakeParameters(p));

      byte[] key = Utility.fromString("document");
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
      streamItr.setContext(sc);

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
        len.delete();
      }

    }
  }

  public void testBigLengths() throws IOException {
    File len = Utility.createTemporary();
    try {

      Parameters p = new Parameters();
      p.set("filename", len.getAbsolutePath());
      DiskLengthsWriter writer = new DiskLengthsWriter(new FakeParameters(p));

      byte[] key = Utility.fromString("document");

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
      len.delete();
    }
  }
}
