/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import org.junit.Test;
import org.lemurproject.galago.core.types.DocumentIndicator;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class DocumentIndicatorWriterTest {
  @Test
  public void testSomeMethod() throws Exception {
    File tmp = FileUtility.createTemporary();
    try {
      Parameters p = Parameters.create();
      p.set("filename", tmp.getAbsolutePath());
      DocumentIndicatorWriter writer = new DocumentIndicatorWriter(new FakeParameters(p));

      Map<Long, Boolean> trueData = new HashMap<Long,Boolean>();

      for (int i = 0; i < 100; i++) {
        trueData.put((long) i, (i % 2 == 0));
        writer.process(new DocumentIndicator(i, (i % 2 == 0)));
      }

      for (long i = 8000000000l; i < 9000010000l; i += 100000001l) {
        trueData.put(i, (i % 2 == 0));
        writer.process(new DocumentIndicator(i, (i % 2 == 0)));
      }

      writer.close();

      DocumentIndicatorReader reader = new DocumentIndicatorReader(tmp.getAbsolutePath());

      DocumentIndicatorReader.KeyIterator ki = reader.getIterator();
      while (!ki.isDone()) {
        long id = Long.parseLong(ki.getKeyString());
        Boolean val = Boolean.parseBoolean(ki.getValueString());
        assertEquals(val, trueData.get(id));
        ki.nextKey();
      }
      reader.close();

    } finally {
      assertTrue(tmp.delete());
    }
  }
}
