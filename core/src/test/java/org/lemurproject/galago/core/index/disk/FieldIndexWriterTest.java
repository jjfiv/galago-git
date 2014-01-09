/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.FieldIndexReader.KeyIterator;
import org.lemurproject.galago.core.index.disk.FieldIndexReader.ListIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class FieldIndexWriterTest extends TestCase {

  public FieldIndexWriterTest(String testName) {
    super(testName);
  }

  public void testFieldIndex() throws Exception {
    File tmp = FileUtility.createTemporary();
    try {
      Parameters p = new Parameters();
      p.set("filename", tmp.getAbsolutePath());
      p.set("tokenizer", new Parameters());
      Parameters t = p.getMap("tokenizer");
      t.set("formats", new Parameters());
      t.getMap("formats").set("test1", "double");
      t.getMap("formats").set("test2", "double");
      FieldIndexWriter writer = new FieldIndexWriter(new FakeParameters(p));

      writer.processFieldName(Utility.fromString("test1"));

      Map<String, String> trueData = new HashMap();

      for (long d = 0; d < 100; d++) {
        writer.processNumber(d);
        writer.processTuple(Utility.fromDouble(d + 0.1));

        trueData.put("" + d, "" + (d + 0.1));
      }

      // big numbers
      writer.processFieldName(Utility.fromString("test2"));
      for (long d = 8000000000L; d < 9000000000L; d += 100000000L) {
        writer.processNumber(d);
        writer.processTuple(Utility.fromDouble(d + 0.1));

        trueData.put("" + d, "" + (d + 0.1));
      }

      writer.close();

      FieldIndexReader reader = new FieldIndexReader(tmp.getAbsolutePath());

      KeyIterator ki = reader.getIterator();
      int keyCount = 0;
      while (!ki.isDone()) {
        keyCount += 1;

        // When we create a FieldSource we can finished this test

//        ListIterator vi = ki.getValueIterator();
//        vi.setContext(new ScoringContext());
//        vi.getContext().document = vi.currentCandidate();
//
//        while(!vi.isDone()){
//        String k = vi.getKeyString();
//        String v = vi.getValueString();
//          System.err.println(k + ", " + v);
//          vi.movePast();
//        }

        ki.nextKey();
      }

      assertEquals(keyCount, 2);

    } finally {
      tmp.delete();
    }
  }
}
