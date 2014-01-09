/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import java.io.File;
import static junit.framework.Assert.assertEquals;
import junit.framework.TestCase;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class DiskNameReverseWriterTest extends TestCase {
  
  public DiskNameReverseWriterTest(String testName) {
    super(testName);
  }

  public void testNamesWriter() throws Exception {
    File tmp = FileUtility.createTemporary();
    try {
      Parameters p = new Parameters();
      p.set("filename", tmp.getAbsolutePath());
      DiskNameReverseWriter writer = new DiskNameReverseWriter(new FakeParameters(p));

      // small numbers
      int count = 0;
      for (int i = 100; i < 200; i++) {
        writer.process(new NumberedDocumentData("d-" + i, "document", "url", i, 1));
        count += 1;
      }

      // big numbers
      for (long i = 8000000000L; i < 9000000000L; i += 10000000L) {
        writer.process(new NumberedDocumentData("d-" + i, "document", "url", i, 1));
        count += 1;
      }

      writer.close();

      DiskNameReverseReader reader = new DiskNameReverseReader(tmp.getAbsolutePath());

      DiskNameReverseReader.KeyIterator ki = reader.getIterator();
      int actual = 0;
      while (!ki.isDone()) {
        assertEquals(ki.getKeyString(), "d-" + ki.getValueString());
        actual += 1;
        ki.nextKey();
      }

      assertEquals(count, actual);

    } finally {
      tmp.delete();
    }
  }
}