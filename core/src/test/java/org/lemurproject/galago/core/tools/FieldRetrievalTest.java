/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class FieldRetrievalTest extends TestCase {

  public FieldRetrievalTest(String testName) {
    super(testName);
  }

  public static void verifyIndexStructures(File indexPath) throws IOException {
    // Check main path
    assertTrue(indexPath.isDirectory());

    // Time to check standard parts

    // doc lengths
    File childPath = new File(indexPath, "lengths");
    assertTrue(childPath.exists());

    // doc names -- there are two files
    childPath = new File(indexPath, "names");
    assertTrue(childPath.exists());
    childPath = new File(indexPath, "names.reverse");
    assertTrue(childPath.exists());

    // corpus -- there is one folder
    childPath = new File(indexPath, "corpus");
    assertTrue(childPath.isDirectory());

    // postings -- there are two files
    childPath = new File(indexPath, "postings");
    assertTrue(childPath.exists());
    childPath = new File(indexPath, "postings.porter");
    assertTrue(childPath.exists());

    // field postings -- there are two files
    childPath = new File(indexPath, "field.porter.title");
    assertTrue(childPath.exists());

    childPath = new File(indexPath, "field.porter.other");
    assertTrue(childPath.exists());

    childPath = new File(indexPath, "field.title");
    assertTrue(childPath.exists());

    childPath = new File(indexPath, "field.other");
    assertTrue(childPath.exists());
  }

  public void testFieldRetrievalPipeline() throws Exception {
    File trecCorpusFile = null;
    File indexFile = null;
    File queryFile = null;

    try {
      String trecCorpus = AppTest.trecDocument("55", "<title>sampled sample</title> <other>This is a sample document</other>")
              + AppTest.trecDocument("59", "<title>another sample</title> <other>This is another document</other>");
      trecCorpusFile = Utility.createTemporary();
      Utility.copyStringToFile(trecCorpus, trecCorpusFile);

      indexFile = Utility.createTemporaryDirectory();
      // now, try to build an index from that
      App.main(new String[]{"build", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(),
                "--tokenizer/fields+title", "--tokenizer/fields+other"});

      // Checks path and components
      verifyIndexStructures(indexFile);

      // try to batch search that index with a no-match string
      String queries = "{ \"queries\" : ["
              + "{ \"number\" : \"1\", \"text\" : \"sample\"},\n"
              + "{ \"number\" : \"2\", \"text\" : \"#combine(#extents:sample:part=field.porter.title())\"},\n"
              + "{ \"number\" : \"3\", \"text\" : \"#combine(#inside(sample #field:title()))\"},\n"
              + "{ \"number\" : \"4\", \"text\" : \"#combine(#inside:noOpt=true(#extents:sample:part=postings() #field:title()))\"},\n"
              + "{ \"number\" : \"5\", \"text\" : \"#combine(#inside(#extents:sample:part=postings() #field:title()))\"}\n"
              + "]}";

      queryFile = Utility.createTemporary();
      Utility.copyStringToFile(queries, queryFile);

      // Smoke test with batch search
      ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
      PrintStream printStream = new PrintStream(byteArrayStream);

      new App().run(new String[]{"batch-search",
                "--index=" + indexFile.getAbsolutePath(),
                queryFile.getAbsolutePath()}, printStream);

      // Now, verify that some stuff exists
      String output = byteArrayStream.toString();
            
      String expectedScores =
              "1 Q0 55 1 -1.17683184 galago\n"
              + "1 Q0 59 2 -1.18048269 galago\n"
              + "2 Q0 55 1 -0.29056168 galago\n"
              + "2 Q0 59 2 -0.29078560 galago\n"
              + "3 Q0 55 1 -0.29056168 galago\n"
              + "3 Q0 59 2 -0.29078560 galago\n"
              + "4 Q0 59 1 -1.87147023 galago\n"
              + "4 Q0 55 2 -1.87213402 galago\n"
              + "5 Q0 59 1 -0.69580676 galago\n"
              + "5 Q0 55 2 -0.69647055 galago\n";
      
      assertEquals(expectedScores, output);

    } finally {
      if (trecCorpusFile != null) {
        trecCorpusFile.delete();
      }
      if (queryFile != null) {
        queryFile.delete();
      }
      if (indexFile != null) {
        Utility.deleteDirectory(indexFile);
      }
    }
  }
}
