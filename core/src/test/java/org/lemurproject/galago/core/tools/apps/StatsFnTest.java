/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Random;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class StatsFnTest extends TestCase {

  public StatsFnTest(String testName) {
    super(testName);
  }

  public void testSomeMethod() throws Exception {
    File input = Utility.createTemporary();
    File index = Utility.createTemporaryDirectory();
    try {
      makeIndex(input, index);

      // test defaulty use:
      ByteArrayOutputStream array = new ByteArrayOutputStream();
      PrintStream printStream = new PrintStream(array);
      (new StatsFn()).run(new String[]{"stats",
                "--index=" + index.getAbsolutePath()}, printStream);
      Parameters results = Parameters.parseString(array.toString());
      assert (results.containsKey("postings.porter"));

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
      assert (results.containsKey("postings.porter"));
      assert (results.containsKey("postings.krovetz"));
      assert (results.containsKey("postings"));

      // test length-based usange
      array = new ByteArrayOutputStream();
      printStream = new PrintStream(array);

      (new StatsFn()).run(new String[]{"stats",
                "--index=" + index.getAbsolutePath(),
                "--field+#lengths:document:part=lengths()",
                "--field+document"
              }, printStream);
      results = Parameters.parseString(array.toString());
      assert (results.containsKey("#lengths:document:part=lengths()"));
      assert (results.containsKey("document"));


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
      assert (results.containsKey("#counts:t1:part=postings()"));
      assert (results.containsKey("#counts:t2:part=postings()"));
      assert (results.containsKey("t3"));

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
      assert (results.containsKey("postings.porter"));
      assert (results.containsKey("postings.krovetz"));
      assert (results.containsKey("postings"));
      assert (results.containsKey("#lengths:document:part=lengths()"));
      assert (results.containsKey("#counts:t1:part=postings()"));
      assert (results.containsKey("#counts:t2:part=postings()"));


    } finally {
      input.delete();
      Utility.deleteDirectory(index);
    }
  }

  public static String trecDocument(String docno, String text) {
    return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n"
            + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
  }

  private void makeIndex(File input, File index) throws Exception {
    Random r = new Random();
    StringBuilder corpus = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      StringBuilder text = new StringBuilder();
      text.append("t").append(i);
      for (int j = 0; j < 100; j++) {
        text.append(" t").append(r.nextInt(100));
//        text.append(" ").append(j);
      }
      corpus.append(trecDocument("doc-" + i, text.toString()));
    }
    Utility.copyStringToFile(corpus.toString(), input);

    Parameters p = new Parameters();
    p.set("inputPath", input.getAbsolutePath());
    p.set("indexPath", index.getAbsolutePath());
    p.set("stemmer", Arrays.asList(new String[]{"krovetz", "porter"}));

    // make enum corpus
    (new BuildIndex()).run(p, System.out);
  }
}
