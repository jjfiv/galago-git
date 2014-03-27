/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import org.junit.Test;
import org.lemurproject.galago.core.types.DocumentFeature;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class DocumentPriorWriterTest {
  @Test
  public void testSomeMethod() throws Exception {
    File tmp = FileUtility.createTemporary();
    try {
      Parameters p = new Parameters();
      p.set("filename", tmp.getAbsolutePath());
      DocumentPriorWriter writer = new DocumentPriorWriter(new FakeParameters(p));

      Map<Long, Double> trueData = new HashMap<Long,Double>();

      for (long i = 0; i < 100; i++) {
        trueData.put(i, i + 0.1);
        writer.process(new DocumentFeature(i, i + 0.1));
      }

      for (long i = 8000000000l; i < 9000010000l; i += 100000001l) {
        trueData.put(i, i + 0.1);
        writer.process(new DocumentFeature(i, i + 0.1));
      }

      writer.close();

      DocumentPriorReader reader = new DocumentPriorReader(tmp.getAbsolutePath());

      DocumentPriorReader.KeyIterator ki = reader.getIterator();
      while (!ki.isDone()) {
        long id = Long.parseLong(ki.getKeyString());
        Double val = Double.parseDouble(ki.getValueString());
        assertEquals(val, trueData.get(id));
        
        ki.nextKey();
      }
      reader.close();
    } finally {
      assertTrue(tmp.delete());
    }
  }
}
