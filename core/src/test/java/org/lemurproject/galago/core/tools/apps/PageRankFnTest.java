/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class PageRankFnTest extends TestCase {

  public PageRankFnTest(String testName) {
    super(testName);
  }

  public void testSomeMethod() throws Exception {
    File tempDir = Utility.createTemporaryDirectory();
    try {
      File input = new File(tempDir, "input.trecweb");
      HarvestLinksFnTest.writeInput(input);

      File galago = new File(tempDir, "galago");
      File jobTmp = new File(tempDir, "jobTmp");

      // run harvest links
      Parameters p = new Parameters();
      p.set("inputPath", input.getAbsolutePath());
      p.set("indri", false);
      p.set("galago", true);
      p.set("outputFolder", galago.getAbsolutePath());
      p.set("galagoDist", 3); // should get 2 output files
      p.set("distrib", 2);
      p.set("galagoJobDir", jobTmp.getAbsolutePath());
      p.set("server", false);

      HarvestLinksFn hl = new HarvestLinksFn();
      hl.run(p, System.out);

      // now run pagerank
      File pagerank = new File(tempDir, "pagerank");
      File jobTmp2 = new File(tempDir, "jobTmp2");

      Parameters p2 = new Parameters();
      p2.set("linkdata", galago.getAbsolutePath());
      p2.set("outputFolder", pagerank.getAbsolutePath());
      p2.set("lambda", 0.5);
      p2.set("maxItr", 10);
      p2.set("server", false);
      p2.set("distrib", 3);
      p2.set("galagoJobDir", jobTmp2.getAbsolutePath());
      p2.set("server", false);

      PageRankFn pg = new PageRankFn();
      pg.run(p2, System.out);

      // now check what was produced
      File out1 = new File(pagerank, "pagerank.docNameOrder");
      String data = Utility.readFileToString(out1);

      // expected output (actual comparison is trucated to 6 decimal points)
      String exp = "test-0 0.12294280112598908\n"
              + "test-1 0.1180018344140897\n"
              + "test-10 0.06151406291755301\n"
              + "test-11 0.05633415708660931\n"
              + "test-2 0.1006919050495244\n"
              + "test-3 0.07177212629157871\n"
              + "test-4 0.09741688722656808\n"
              + "test-5 0.09641894694537836\n"
              + "test-6 0.06439791377362236\n"
              + "test-7 0.0774668462267146\n"
              + "test-8 0.07367786713543674\n"
              + "test-9 0.05936465180693555\n";
      checkOutput(data, exp);

      File out2 = new File(pagerank, "pagerank.scoreOrder");
      data = Utility.readFileToString(out2);

      // expected output (actual comparison is trucated to 6 decimal points)
      exp = "test-0 0.12294280112598908\n"
              + "test-1 0.1180018344140897\n"
              + "test-2 0.1006919050495244\n"
              + "test-4 0.09741688722656808\n"
              + "test-5 0.09641894694537836\n"
              + "test-7 0.0774668462267146\n"
              + "test-8 0.07367786713543674\n"
              + "test-3 0.07177212629157871\n"
              + "test-6 0.06439791377362236\n"
              + "test-10 0.06151406291755301\n"
              + "test-9 0.05936465180693555\n"
              + "test-11 0.05633415708660931\n";
      checkOutput(data, exp);

    } finally {
      Utility.deleteDirectory(tempDir);

    }
  }

  private void checkOutput(String data, String expected) {
    String[] dataLines = data.split("\n");
    String[] expLines = expected.split("\n");
    assertEquals(expLines.length, dataLines.length);

    for (int i = 0; i < dataLines.length; i++) {
      if (expLines[i].contains(" ")) {
        String[] exp = expLines[i].split(" ");
        String[] dat = dataLines[i].split(" ");

        assertEquals(exp[0], dat[0]);
        assertEquals(Double.parseDouble(exp[1]), Double.parseDouble(dat[1]), 0.000001);
      }
    }
  }
}
