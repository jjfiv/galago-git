/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.junit.Test;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class StatsFnTest {
  @Test
  public void testSomeMethod() throws Exception {
    File input = FileUtility.createTemporary();
    File index = FileUtility.createTemporaryDirectory();
    try {
      makeIndex(input, index);

      // test defaulty use:
      ByteArrayOutputStream array = new ByteArrayOutputStream();
      PrintStream printStream = new PrintStream(array);
      (new StatsFn()).run(new String[]{"stats",
                "--index=" + index.getAbsolutePath()}, printStream);
      Parameters results = Parameters.parseString(array.toString());
      assert (results.containsKey("postings.krovetz"));

      // test part-based usange
      array = new ByteArrayOutputStream();
      printStream = new PrintStream(array);

      (new StatsFn()).run(new String[]{"stats",
                "--index=" + index.getAbsolutePath(),
                "--part+postings.porter",
                "--part+postings.krovetz",
                "--part+postings"
              }, printStream);
      results = Parameters.parseString(array.toString());
      assertTrue(results.containsKey("postings.porter"));
      assertTrue (results.containsKey("postings.krovetz"));
      assertTrue(results.containsKey("postings"));

      // test length-based usange
      array = new ByteArrayOutputStream();
      printStream = new PrintStream(array);

      (new StatsFn()).run(new String[]{"stats",
                "--index=" + index.getAbsolutePath(),
                "--field+#lengths:document:part=lengths()",
                "--field+document"
              }, printStream);
      results = Parameters.parseString(array.toString());
      assertTrue(results.containsKey("#lengths:document:part=lengths()"));
      assertTrue (results.containsKey("document"));


      // test node-based usange
      array = new ByteArrayOutputStream();
      printStream = new PrintStream(array);

      (new StatsFn()).run(new String[]{"stats",
                "--index=" + index.getAbsolutePath(),
                "--node+#counts:t1:part=postings()",
                "--node+#counts:t2:part=postings()",
                "--node+t3"
              }, printStream);
      results = Parameters.parseString(array.toString());
      assertTrue (results.containsKey("#counts:t1:part=postings()"));
      assertTrue (results.containsKey("#counts:t2:part=postings()"));
      assertTrue (results.containsKey("t3"));

      // test mixed usange
      array = new ByteArrayOutputStream();
      printStream = new PrintStream(array);

      (new StatsFn()).run(new String[]{"stats",
                "--index=" + index.getAbsolutePath(),
                "--part+postings.porter",
                "--part+postings.krovetz",
                "--part+postings",
                "--node+#counts:t1:part=postings()",
                "--node+#counts:t2:part=postings()",
                "--field+#lengths:document:part=lengths()"
              }, printStream);
      results = Parameters.parseString(array.toString());
      assertTrue (results.containsKey("postings.porter"));
      assertTrue (results.containsKey("postings.krovetz"));
      assertTrue (results.containsKey("postings"));
      assertTrue (results.containsKey("#lengths:document:part=lengths()"));
      assertTrue (results.containsKey("#counts:t1:part=postings()"));
      assertTrue (results.containsKey("#counts:t2:part=postings()"));


    } finally {
      input.delete();
      Utility.deleteDirectory(index);
    }
  }

  private void makeIndex(File input, File index) throws Exception {
    Random r = new Random();
    StringBuilder corpus = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      StringBuilder text = new StringBuilder();
      text.append("t").append(i);
      for (int j = 0; j < 100; j++) {
        text.append(" t").append(r.nextInt(100));
      }
      corpus.append(AppTest.trecDocument("doc-" + i, text.toString()));
    }
    Utility.copyStringToFile(corpus.toString(), input);

    Parameters p = Parameters.instance();
    p.set("inputPath", input.getAbsolutePath());
    p.set("indexPath", index.getAbsolutePath());
    p.set("stemmer", Arrays.asList(new String[]{"krovetz", "porter"}));

    // make enum corpus
    (new BuildIndex()).run(p, System.out);
  }
}
