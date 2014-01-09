/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index.disk;

import java.io.File;
import java.util.Random;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class IndexReaderSkipTest extends TestCase {

  public IndexReaderSkipTest(String name) {
    super(name);
  }

  public void testPositionIndexSkipping() throws Exception {
    Random r = new Random();
    File temp = FileUtility.createTemporary();

    try {
      Parameters parameters = new Parameters();
      parameters.set("filename", temp.getAbsolutePath());
      parameters.set("skipDistance", 10);
      parameters.set("skipResetDistance", 5);
      PositionIndexWriter writer = new PositionIndexWriter(new FakeParameters(parameters));

      writer.processWord(Utility.fromString("key"));
      for (int doc = 0; doc < 1000; doc++) {
        writer.processDocument(doc);
        for (int begin = 0; begin < 100; begin++) {
          writer.processPosition(begin);
        }
      }
      writer.close();

      PositionIndexReader reader = new PositionIndexReader(parameters.getString("filename"));
      for (int i = 1; i < 1000; i++) {
        BaseIterator extents = reader.getTermExtents("key");
        extents.syncTo(i);
        assertEquals(extents.currentCandidate(), i);

        BaseIterator counts = reader.getTermCounts("key");
        counts.syncTo(i);
        assertEquals(counts.currentCandidate(), i);
      }

    } finally {
      temp.delete();
    }
  }

  public void testCountIndexSkipping() throws Exception {
    Random r = new Random();
    File temp = FileUtility.createTemporary();

    try {
      Parameters parameters = new Parameters();
      parameters.set("filename", temp.getAbsolutePath());
      parameters.set("skipDistance", 10);
      parameters.set("skipResetDistance", 5);
      CountIndexWriter writer = new CountIndexWriter(new FakeParameters(parameters));

      writer.processWord(Utility.fromString("key"));
      for (int doc = 0; doc < 1000; doc++) {
        writer.processDocument(doc);
        writer.processTuple(r.nextInt(128) + 128);
      }
      writer.close();

      CountIndexReader reader = new CountIndexReader(parameters.getString("filename"));
      for (int i = 1; i < 1000; i++) {
        BaseIterator counts = reader.getTermCounts("key");
        counts.syncTo(i);
        assertEquals(counts.currentCandidate(), i);
      }

    } finally {
      temp.delete();
    }
  }

  public void testWindowIndexSkipping() throws Exception {
    Random r = new Random();
    File temp = FileUtility.createTemporary();

    try {
      Parameters parameters = new Parameters();
      parameters.set("filename", temp.getAbsolutePath());
      parameters.set("skipDistance", 10);
      parameters.set("skipResetDistance", 5);
      WindowIndexWriter writer = new WindowIndexWriter(new FakeParameters(parameters));

      writer.processExtentName(Utility.fromString("key"));
      for (int doc = 0; doc < 1000; doc++) {
        writer.processNumber(doc);
        for (int begin = 0; begin < 100; begin++) {
          writer.processBegin(begin);
          writer.processTuple(begin + r.nextInt(128) + 128); // end is between 128 and 256 after begin
        }
      }
      writer.close();

      WindowIndexReader reader = new WindowIndexReader(parameters.getString("filename"));
      for (int i = 1; i < 1000; i++) {
        BaseIterator extents = reader.getTermExtents("key");
        extents.syncTo(i);
        assertEquals(extents.currentCandidate(), i);

        BaseIterator counts = reader.getTermCounts("key");
        counts.syncTo(i);
        assertEquals(counts.currentCandidate(), i);
      }

    } finally {
      temp.delete();
    }
  }
}
