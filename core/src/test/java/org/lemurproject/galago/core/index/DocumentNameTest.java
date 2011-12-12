// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.index.disk.DiskNameWriter;
import org.lemurproject.galago.core.index.disk.DiskNameReader;
import java.io.File;

import junit.framework.TestCase;

import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DocumentNameTest extends TestCase {

  public DocumentNameTest(String testName) {
    super(testName);
  }

  public void testDocumentNameStore() throws Exception {
    File f = null;
    try {
      Parameters p = new Parameters();
      f = Utility.createTemporaryDirectory();
      File names = new File(f.getAbsolutePath() + File.separator + "names");
      p.set("filename", names.getAbsolutePath());
      FakeParameters params = new FakeParameters(p);
      DiskNameWriter writer = new DiskNameWriter(params);

      for (int key_val = 10; key_val < 35; key_val++) {
        String str_val = "document_name_key_is_" + key_val;
        NumberedDocumentData ndd = new NumberedDocumentData(str_val, "", "", key_val, 0);
        writer.process(ndd);
      }
      writer.close();

      DiskNameReader reader = new DiskNameReader(names.getAbsolutePath());
      DiskNameReader revReader = new DiskNameReader(names.getAbsolutePath() + ".reverse");

      int key = 15;
      String name = "document_name_key_is_" + key;

      String result1 = reader.getDocumentName(key);
      assert name.equals(result1);

      int result2 = revReader.getDocumentIdentifier(name);
      assert key == result2;
    } finally {
      if (f != null) {
        Utility.deleteDirectory(f);
      }
    }
  }
}
