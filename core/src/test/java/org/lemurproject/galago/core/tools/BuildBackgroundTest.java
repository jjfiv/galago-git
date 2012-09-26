// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

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
    File indexFile1 = null;
    File backgroundIndex = null;
    File queryFile = null;
    try {

      String trecCorpus1 = trecDocument("55", "This is a sample document")
              + trecDocument("59", "sample document two");

      String trecCorpus2 = trecDocument("55", "This is a sample document")
              + trecDocument("59", "sample document two")
              + trecDocument("73", "samples document")
              + trecDocument("73", "samples document two ")
              + trecDocument("73", "samples document is a")
              + trecDocument("73", "samples document sample")
              + trecDocument("73", "samples document document")
              + trecDocument("82", "sample eight two");

      trecCorpusFile1 = Utility.createTemporary();
      trecCorpusFile2 = Utility.createTemporary();
      Utility.copyStringToFile(trecCorpus1, trecCorpusFile1);
      Utility.copyStringToFile(trecCorpus2, trecCorpusFile2);

      assertTrue(trecCorpusFile1.exists());
      assertTrue(trecCorpusFile2.exists());

      indexFile1 = Utility.createTemporary();
      indexFile1.delete();
      App.main(new String[]{"build", "--indexPath=" + indexFile1.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile1.getAbsolutePath()});

      App.main(new String[]{"build-background", "--indexPath=" + indexFile1.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile2.getAbsolutePath(),
                "--nonStemmedPostings=false", "--stemmedPostings=true",
                "--partName=background", "--stemmer+porter"});

      backgroundIndex = Utility.createTemporary();
      backgroundIndex.delete();
      App.main(new String[]{"build", "--indexPath=" + backgroundIndex.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile2.getAbsolutePath()});


      assertFalse(new File(indexFile1.getAbsolutePath() + File.separator + "background").exists());
      assertTrue(new File(indexFile1.getAbsolutePath() + File.separator + "background.porter").exists());

      // try to batch search that index with a no-match string
      String queries_reg = "{ \"index\" : \"" + indexFile1.getAbsolutePath() + "\", \"queries\" : ["
              + "{ \"number\" : \"2\", \"text\": \"#combine( #feature:dirichlet( #counts:two:part=postings.porter() ) #feature:dirichlet( #counts:sample:part=postings.porter() ) )\"},"
              + "{ \"number\" : \"9\", \"text\" : \"#combine( #feature:dirichlet( #counts:sample:part=postings.porter() ) )\"},"
              + "{ \"number\" : \"11\", \"text\" : \"#combine( #feature:dirichlet( #counts:is:part=postings.porter() ) #feature:dirichlet( #counts:two:part=postings.porter() ) )\"},"
//              + "]}";
              + "], \"printTransformation\":true}";

      queryFile = Utility.createTemporary();
      Utility.copyStringToFile(queries_reg, queryFile);
      String expected_reg =
              "2 Q0 59 1 -1.73087481 galago\n"
              + "2 Q0 55 2 -1.73486418 galago\n"
              + "9 Q0 59 1 -1.38562925 galago\n"
              + "9 Q0 55 2 -1.38695903 galago\n"
              + "11 Q0 59 1 -2.07877996 galago\n"
              + "11 Q0 55 2 -2.08010975 galago\n";
      runQueries(queryFile, expected_reg);
      
//  DEPRECATED - need to rethink a bit before re-enabling
//      String queries_back1 = "{ \"index\" : \"" + indexFile1.getAbsolutePath() + "\", \"queries\" : ["
//              + "{ \"number\" : \"2\", \"text\": \"#combine( #feature:dirichlet( #counts:two:part=postings.porter() ) #feature:dirichlet( #counts:sample:part=postings.porter() ) )\"},"
//              + "{ \"number\" : \"9\", \"text\" : \"#combine( #feature:dirichlet( #counts:sample:part=postings.porter() ) )\"},"
//              + "{ \"number\" : \"11\", \"text\" : \"#combine( #feature:dirichlet( #counts:is:part=postings.porter() ) #feature:dirichlet( #counts:two:part=postings.porter() ) )\"},"
//              + "], \"backgroundPartMap\" : {\"postings.porter\" : \"background.porter\"}}";
//
//      Utility.copyStringToFile(queries_back1, queryFile);
//      runQueries(queryFile, expected_back);

      String queries_back2 = "{ \"index\" : {\"reg\" : \"" + indexFile1.getAbsolutePath() + "\","
              + "\"back\" : \"" + backgroundIndex.getAbsolutePath() + "\" }, "
              + "\"defaultGroup\" : \"reg\","
              + "\"queries\" : ["
              + " { \"number\" : \"2\", \"text\": \"#combine( #feature:dirichlet( #counts:two:part=postings.porter() ) #feature:dirichlet( #counts:sample:part=postings.porter() ) )\"},"
              + " { \"number\" : \"9\", \"text\" : \"#combine( #feature:dirichlet( #counts:sample:part=postings.porter() ) )\"},"
              + " { \"number\" : \"11\", \"text\" : \"#combine( #feature:dirichlet( #counts:is:part=postings.porter() ) #feature:dirichlet( #counts:two:part=postings.porter() ) )\"},"
              + "],"
              + "\"backgroundIndex\" : \"back\"}";

      Utility.copyStringToFile(queries_back2, queryFile);

      String expected_back =
              "2 Q0 59 1 -1.60833350 galago\n"
              + "2 Q0 55 2 -1.61254386 galago\n"
              + "9 Q0 59 1 -1.06094589 galago\n"
              + "9 Q0 55 2 -1.06227568 galago\n"
              + "11 Q0 55 1 -2.36122993 galago\n"
              + "11 Q0 59 2 -2.36133423 galago\n";
      
      runQueries(queryFile, expected_back);

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
      if (indexFile1 != null) {
        Utility.deleteDirectory(indexFile1);
      }
      if (backgroundIndex != null) {
        Utility.deleteDirectory(backgroundIndex);
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
