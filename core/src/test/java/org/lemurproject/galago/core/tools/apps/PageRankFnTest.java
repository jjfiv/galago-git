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
      p2.set("distrib", 2);
      p2.set("galagoJobDir", jobTmp2.getAbsolutePath());
      p2.set("server", false);
      p2.set("delta", 0.000000001);
      
      PageRankFn pg = new PageRankFn();
      pg.run(p2, System.out);

      // now check what was produced
      File out1 = new File(pagerank, "pagerank.docNameOrder");
      String data = Utility.readFileToString(out1);

      // expected output (actual comparison is trucated to 6 decimal points)
      String exp = "test-0 0.14044514319221924\n"
              + "test-1 0.10091959505482992\n"
              + "test-10 0.05115981927289599\n"
              + "test-11 0.05115981927289599\n"
              + "test-2 0.08488240356450753\n"
              + "test-3 0.08673666439408949\n"
              + "test-4 0.07918772966163073\n"
              + "test-5 0.0954103007236506\n"
              + "test-6 0.07937310923332822\n"
              + "test-7 0.07387799299215828\n"
              + "test-8 0.07558165579260986\n"
              + "test-9 0.08126576684518416\n";

      checkOutput(data, exp);

      File out2 = new File(pagerank, "pagerank.scoreOrder");
      data = Utility.readFileToString(out2);

      // expected output (actual comparison is trucated to 6 decimal points)
      exp = "test-0 0.14044514319221924\n"
              + "test-1 0.10091959505482992\n"
              + "test-5 0.0954103007236506\n"
              + "test-3 0.08673666439408949\n"
              + "test-2 0.08488240356450753\n"
              + "test-9 0.08126576684518416\n"
              + "test-6 0.07937310923332822\n"
              + "test-4 0.07918772966163073\n"
              + "test-8 0.07558165579260986\n"
              + "test-7 0.07387799299215828\n"
              + "test-10 0.05115981927289599\n"
              + "test-11 0.05115981927289599\n";

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
