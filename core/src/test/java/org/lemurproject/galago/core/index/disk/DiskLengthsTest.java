/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.DiskLengthsReader.KeyIterator;
import org.lemurproject.galago.core.index.disk.DiskLengthsReader.MemoryMapLengthsIterator;
import org.lemurproject.galago.core.index.disk.DiskLengthsReader.StreamLengthsIterator;
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
      MemoryMapLengthsIterator memItr = ki.getMemoryValueIterator();
      StreamLengthsIterator streamItr = ki.getStreamValueIterator();

      while (!memItr.isDone() || !streamItr.isDone()) {
        
        assertEquals(memItr.getCurrentIdentifier(), streamItr.getCurrentIdentifier());
        assertEquals(memItr.getCurrentLength(), streamItr.getCurrentLength());
          
        memItr.next();
        streamItr.next();
      }
      
      memItr.reset();
      streamItr.reset();
      
      memItr.moveTo(50);
      streamItr.moveTo(50);
      assertEquals(memItr.currentCandidate(), 50);
      assertEquals(streamItr.currentCandidate(), 50);
      assertEquals(memItr.getCurrentLength(), 51);
      assertEquals(streamItr.getCurrentLength(), 51);

      memItr.moveTo(90);
      streamItr.moveTo(90);
      assertEquals(memItr.currentCandidate(), 90);
      assertEquals(streamItr.currentCandidate(), 90);
      assertEquals(memItr.getCurrentLength(), 91);
      assertEquals(streamItr.getCurrentLength(), 91);
      
      memItr.moveTo(90);
      streamItr.moveTo(90);
      assertEquals(memItr.currentCandidate(), 90);
      assertEquals(streamItr.currentCandidate(), 90);
      assertEquals(memItr.getCurrentLength(), 91);
      assertEquals(streamItr.getCurrentLength(), 91);

      memItr.moveTo(110);
      streamItr.moveTo(110);
      assertEquals(memItr.currentCandidate(), 110);
      assertEquals(streamItr.currentCandidate(), 110);
      assertEquals(memItr.getCurrentLength(), 111);
      assertEquals(streamItr.getCurrentLength(), 111);
      
      memItr.moveTo(200);
      streamItr.moveTo(200);
      assertEquals(memItr.currentCandidate(), 110);
      assertEquals(streamItr.currentCandidate(), 110);
      assertEquals(memItr.getCurrentLength(), 111);
      assertEquals(streamItr.getCurrentLength(), 111);
      
      reader.close();


    } finally {
      if (len != null) {
        len.delete();
      }

    }
  }
}
