/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class BuildFilteredCollectionTest extends TestCase {

  public BuildFilteredCollectionTest(String testName) {
    super(testName);
  }

  public void testSomeMethod() throws Exception {
    File input = Utility.createTemporary();
    File filter = Utility.createTemporary();
    File output = Utility.createTemporaryDirectory();
    try {
      makeTrecDocs(input);

      String docs = "doc-1\ndoc-2\ndoc-88\ndoc-99\ndoc-100";
      Utility.copyStringToFile(docs, filter);

      Parameters p = new Parameters();
      p.set("inputPath", input.getAbsolutePath());
      p.set("outputPath", output.getAbsolutePath());
      p.set("filter", filter.getAbsolutePath());
      p.set("distrib", 1);
      p.set("mode", "local");
      (new BuildFilteredCollection()).run(p, System.out);

      int dcount = 0;
      for (File o : output.listFiles()) {
        dcount += countDocuments(o);
      }
      
      assertEquals(dcount, 4);

    } finally {
      input.delete();
      filter.delete();
      Utility.deleteDirectory(output);
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

  public static String trecDocument(String docno, String text) {
    return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n"
            + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
  }

  public static int countDocuments(File f) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f))));
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
