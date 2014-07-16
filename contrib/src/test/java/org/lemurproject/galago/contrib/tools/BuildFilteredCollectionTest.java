/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.lemurproject.galago.contrib.util.TestingUtils.trecDocument;

/**
 *
 * @author sjh
 */
public class BuildFilteredCollectionTest {
  @Test
  public void testSomeMethod() throws Exception {
    File input = FileUtility.createTemporary();
    File filter = FileUtility.createTemporary();
    File output = FileUtility.createTemporaryDirectory();
    try {
      makeTrecDocs(input);

      String docs = "doc-1\ndoc-2\ndoc-88\ndoc-99\ndoc-100";
      Utility.copyStringToFile(docs, filter);

      Parameters p = Parameters.instance();
      p.set("inputPath", input.getAbsolutePath());
      p.set("outputPath", output.getAbsolutePath());
      p.set("filter", filter.getAbsolutePath());
      p.set("distrib", 1);
      p.set("mode", "local");
      (new BuildFilteredCollection()).run(p, System.out);

      int dcount = 0;
      assertNotNull(output);
      assertNotNull(output.listFiles());
      for (File o : FileUtility.safeListFiles(output)) {
        // System.err.println(o);
        dcount += countDocuments(o);
      }
      
      assertEquals(dcount, 4);

    } finally {
      input.delete();
      filter.delete();
      FSUtil.deleteDirectory(output);
    }
  }

  private void makeTrecDocs(File input) throws Exception {
    Random r = new Random();
    StringBuilder corpus = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      StringBuilder text = new StringBuilder();
      for (int j = 0; j < 100; j++) {
        text.append(" ").append(r.nextInt(100));
//        text.append(" ").append(j);
      }
      corpus.append(trecDocument("doc-" + i, text.toString()));
    }
    Utility.copyStringToFile(corpus.toString(), input);
  }

  public static int countDocuments(File f) throws IOException {
    BufferedReader reader = Utility.utf8Reader(f);
    String line;
    int count = 0;
    while (null != (line = reader.readLine())) {
      if (line.startsWith("<DOC>")) {
        count += 1;
      }
    }
    reader.close();
    return count;
  }
}
