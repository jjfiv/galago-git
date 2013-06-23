/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.DiskNameReader.KeyIterator;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DiskNameWriterTest extends TestCase {

  public DiskNameWriterTest(String testName) {
    super(testName);
  }

  public void testNamesWriter() throws Exception {
    File tmp = Utility.createTemporary();
    try {
      Parameters p = new Parameters();
      p.set("filename", tmp.getAbsolutePath());
      DiskNameWriter writer = new DiskNameWriter(new FakeParameters(p));

      // small numbers
      int count = 0;
      for (int i = 0; i < 100; i++) {
        writer.process(new NumberedDocumentData("d-" + i, "document", "url", i, 1));
        count += 1;
      }

      // big numbers
      for (long i = 8000000000L; i < 9000000000L; i += 10000000L) {
        writer.process(new NumberedDocumentData("d-" + i, "document", "url", i, 1));
        count += 1;
      }

      writer.close();

      DiskNameReader reader = new DiskNameReader(tmp.getAbsolutePath());

      KeyIterator ki = reader.getIterator();
      int actual = 0;
      while (!ki.isDone()) {
        assertEquals("d-" + ki.getKeyString(), ki.getValueString());
        actual += 1;
        ki.nextKey();
      }

      assertEquals(count, actual);

    } finally {
      tmp.delete();
    }
  }
}
