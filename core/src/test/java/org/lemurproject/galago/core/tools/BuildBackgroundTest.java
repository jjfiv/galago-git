// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import org.lemurproject.galago.core.tools.App;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class BuildBackgroundTest extends TestCase {

  public BuildBackgroundTest(String testName) {
    super(testName);
  }

  public static String trecDocument(String docno, String text) {
    return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n"
            + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
  }

  public void testBackgrounds() throws Exception {
    File trecCorpusFile1 = null;
    File trecCorpusFile2 = null;
    File indexFile = null;
    File queryFile = null;
    try {

      String trecCorpus1 = trecDocument("55", "This is a sample document")
              + trecDocument("59", "sample document two")
              + trecDocument("73", "samples document")
              + trecDocument("73", "samples document two ")
              + trecDocument("73", "samples document is a")
              + trecDocument("73", "samples document sample")
              + trecDocument("73", "samples document document")
              + trecDocument("82", "sample eight two");

      String trecCorpus2 = trecDocument("55", "This is a sample document")
              + trecDocument("59", "sample document two");

      trecCorpusFile1 = Utility.createTemporary();
      trecCorpusFile2 = Utility.createTemporary();
      Utility.copyStringToFile(trecCorpus1, trecCorpusFile1);
      Utility.copyStringToFile(trecCorpus2, trecCorpusFile2);

      assertTrue(trecCorpusFile1.exists());
      assertTrue(trecCorpusFile2.exists());

      indexFile = Utility.createTemporary();
      indexFile.delete();
      App.main(new String[]{"build", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile2.getAbsolutePath()});

      App.main(new String[]{"build-background", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile1.getAbsolutePath(),
                "--nonStemmedPostings=false", "--stemmedPostings=true",
                "--partName=background", "--stemmer+null", "--stemmer+porter"});

      assertFalse(new File(indexFile.getAbsolutePath() + File.separator + "background").exists());
      assertTrue(new File(indexFile.getAbsolutePath() + File.separator + "background.null").exists());
      assertTrue(new File(indexFile.getAbsolutePath() + File.separator + "background.porter").exists());

      // try to batch search that index with a no-match string
      String queries = "{ \"index\" : \"" + indexFile.getAbsolutePath() + "\", \"queries\" : ["
              + "{ \"number\" : \"2a\", \"text\": \"#combine( #feature:dirichlet( #counts:two:part=postings.porter() ) #feature:dirichlet( #counts:sample:part=postings.porter() ) )\"},"
              + "{ \"number\" : \"2b\", \"text\" : \"#combine( #feature:dirichlet( #counts:two:lm=background.null:part=postings.porter() ) #feature:dirichlet( #counts:sample:lm=background.null:part=postings.porter() ) )\"},"
              + "{ \"number\" : \"2c\", \"text\" : \"#combine( #feature:dirichlet( #counts:two:lm=background.porter:part=postings.porter() ) #feature:dirichlet( #counts:sample:lm=background.porter:part=postings.porter() ) )\"},"
              + "{ \"number\" : \"9a\", \"text\" : \"#combine( #feature:dirichlet( #counts:sample:part=postings.porter() ) )\"},"
              + "{ \"number\" : \"9b\", \"text\" : \"#combine( #feature:dirichlet( #counts:sample:lm=background.null:part=postings.porter() ) )\"},"
              + "{ \"number\" : \"9c\", \"text\" : \"#combine( #feature:dirichlet( #counts:sample:lm=background.porter:part=postings.porter() ) )\"},"
              + "{ \"number\" : \"11a\", \"text\" : \"#combine( #feature:dirichlet( #counts:is:part=postings.porter() ) #feature:dirichlet( #counts:two:part=postings.porter() ) )\"},"
              + "{ \"number\" : \"11b\", \"text\" : \"#combine( #feature:dirichlet( #counts:is:lm=background.null:part=postings.porter() ) #feature:dirichlet( #counts:two:lm=background.null:part=postings.porter() ) )\"},"
              + "{ \"number\" : \"11c\", \"text\" : \"#combine( #feature:dirichlet( #counts:is:lm=background.porter:part=postings.porter() ) #feature:dirichlet( #counts:two:lm=background.porter:part=postings.porter() ) )\"}"
              + "]}";

      queryFile = Utility.createTemporary();
      Utility.copyStringToFile(queries, queryFile);

      String expected =
              "2a Q0 59 1 -1.73087481 galago\n"
              + "2a Q0 55 2 -1.73486418 galago\n"
              + "2b Q0 59 1 -2.01259865 galago\n"
              + "2b Q0 55 2 -2.01680902 galago\n"
              + "2c Q0 59 1 -1.60833350 galago\n"
              + "2c Q0 55 2 -1.61254386 galago\n"
              + "9a Q0 59 1 -1.38562925 galago\n"
              + "9a Q0 55 2 -1.38695903 galago\n"
              + "9b Q0 59 1 -1.86947621 galago\n"
              + "9b Q0 55 2 -1.87080600 galago\n"
              + "9c Q0 59 1 -1.06094589 galago\n"
              + "9c Q0 55 2 -1.06227568 galago\n"
              + "11a Q0 59 1 -2.07877996 galago\n"
              + "11a Q0 55 2 -2.08010975 galago\n"
              + "11b Q0 55 1 -2.36122993 galago\n"
              + "11b Q0 59 2 -2.36133423 galago\n"
              + "11c Q0 55 1 -2.36122993 galago\n"
              + "11c Q0 59 2 -2.36133423 galago\n";

      runQueries(queryFile, expected);

    } finally {
      if (trecCorpusFile1 != null) {
        trecCorpusFile1.delete();
      }
      if (trecCorpusFile2 != null) {
        trecCorpusFile2.delete();
      }
      if (queryFile != null) {
        queryFile.delete();
      }
      if (indexFile != null) {
        Utility.deleteDirectory(indexFile);
      }
    }
  }

  public void runQueries(File queries, String expected) throws Exception {
    // Smoke test with batch search
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    new App().run(new String[]{"batch-search",
              queries.getAbsolutePath()}, printStream);

    String output = byteArrayStream.toString();

    assertEquals(expected, output);
  }
}
