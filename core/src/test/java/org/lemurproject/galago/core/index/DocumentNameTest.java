// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseWriter;
import org.lemurproject.galago.core.index.disk.DiskNameWriter;
import org.lemurproject.galago.core.types.DocumentNameId;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class DocumentNameTest {
  @Test
  public void testDocumentNameStore() throws Exception {
    File f = null;
    try {
      Parameters p = Parameters.create();
      f = FileUtility.createTemporaryDirectory();
      File names = new File(f.getAbsolutePath() + File.separator + "names");
      p.set("filename", names.getAbsolutePath());
      FakeParameters params = new FakeParameters(p);
      DiskNameWriter writer = new DiskNameWriter(params);

      for (int key_val = 10; key_val < 35; key_val++) {
        String str_val = "document_name_key_is_" + key_val;
        writer.process(new DocumentNameId(ByteUtil.fromString(str_val), key_val));
      }
      writer.close();

      DiskNameReader reader = new DiskNameReader(names.getAbsolutePath());

      int key = 15;
      String name = "document_name_key_is_" + key;

      String result1 = reader.getDocumentName(key);
      assertEquals(name, result1);

    } finally {
      if (f != null) {
        FSUtil.deleteDirectory(f);
      }
    }
  }

  @Test
  public void testDocumentNameReverseStore() throws Exception {
    File f = null;
    try {
      Parameters p = Parameters.create();
      f = FileUtility.createTemporaryDirectory();
      File names = new File(f.getAbsolutePath() + File.separator + "names.reverse");
      p.set("filename", names.getAbsolutePath());
      FakeParameters params = new FakeParameters(p);
      DiskNameReverseWriter writer = new DiskNameReverseWriter(params);

      for (int key_val = 10; key_val < 35; key_val++) {
        String str_val = "document_name_key_is_" + key_val;
        writer.process(new DocumentNameId(ByteUtil.fromString(str_val), key_val));
      }
      writer.close();

      DiskNameReverseReader reader = new DiskNameReverseReader(names.getAbsolutePath());

      int key = 15;
      String name = "document_name_key_is_" + key;

      long result2 = reader.getDocumentIdentifier(name);
      assertEquals(key, result2);
    } finally {
      if (f != null) {
        FSUtil.deleteDirectory(f);
      }
    }
  }
}
