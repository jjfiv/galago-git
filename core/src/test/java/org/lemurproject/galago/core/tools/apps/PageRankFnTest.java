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

      String exp = "test-0 0.1229428011\n"
              + "test-1 0.1180018344\n"
              + "test-10 0.0615140629\n"
              + "test-11 0.0563341571\n"
              + "test-2 0.1006919050\n"
              + "test-3 0.0717721263\n"
              + "test-4 0.0974168872\n"
              + "test-5 0.0964189469\n"
              + "test-6 0.0643979138\n"
              + "test-7 0.0774668462\n"
              + "test-8 0.0736778671\n"
              + "test-9 0.0593646518\n";
      assertEquals(data, exp);


      File out2 = new File(pagerank, "pagerank.scoreOrder");
      data = Utility.readFileToString(out2);
      exp = "test-0 0.1229428011\n"
              + "test-1 0.1180018344\n"
              + "test-2 0.1006919050\n"
              + "test-4 0.0974168872\n"
              + "test-5 0.0964189469\n"
              + "test-7 0.0774668462\n"
              + "test-8 0.0736778671\n"
              + "test-3 0.0717721263\n"
              + "test-6 0.0643979138\n"
              + "test-10 0.0615140629\n"
              + "test-9 0.0593646518\n"
              + "test-11 0.0563341571\n";
      assertEquals(data, exp);

    } finally {

      Utility.deleteDirectory(tempDir);
    }
  }
}
