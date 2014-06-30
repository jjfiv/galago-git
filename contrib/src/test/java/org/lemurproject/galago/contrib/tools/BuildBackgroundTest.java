// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.tools;

import org.junit.Assert;
import org.junit.Test;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.json.JSONUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.lemurproject.galago.contrib.util.TestingUtils.trecDocument;

/**
 *
 * @author sjh
 */
public class BuildBackgroundTest  {

  final String newLine = System.getProperty("line.separator");
      
  @Test
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

      trecCorpusFile1 = FileUtility.createTemporary();
      trecCorpusFile2 = FileUtility.createTemporary();
      Utility.copyStringToFile(trecCorpus1, trecCorpusFile1);
      Utility.copyStringToFile(trecCorpus2, trecCorpusFile2);

      assertTrue(trecCorpusFile1.exists());
      assertTrue(trecCorpusFile2.exists());

      indexFile1 = FileUtility.createTemporary();
      assertTrue(indexFile1.delete());
      App.main(new String[]{"build", "--indexPath=" + indexFile1.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile1.getAbsolutePath()});

      App.main(new String[]{"build-coll-background", "--indexPath=" + indexFile1.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile2.getAbsolutePath(),
                "--partName=background", "--stemmer=krovetz"});

      backgroundIndex = FileUtility.createTemporary();
      assertTrue(backgroundIndex.delete());
      App.main(new String[]{"build", "--indexPath=" + backgroundIndex.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile2.getAbsolutePath()});


      assertTrue(new File(indexFile1.getAbsolutePath() + File.separator + "background").exists());

      // try to batch search that index with a no-match string
      String queries_reg = "{ \"index\" : \"" + JSONUtil.escape(indexFile1.getAbsolutePath()) + "\", \"queries\" : ["
              + "{ \"number\" : \"2\", \"text\": \"#combine( #dirichlet( #counts:two:part=postings.krovetz() ) #dirichlet( #counts:sample:part=postings.krovetz() ) )\"},"
              + "{ \"number\" : \"9\", \"text\" : \"#combine( #dirichlet( #counts:sample:part=postings.krovetz() ) )\"},"
              + "{ \"number\" : \"11\", \"text\" : \"#combine( #dirichlet( #counts:is:part=postings.krovetz() ) #dirichlet( #counts:two:part=postings.krovetz() ) )\"},"
              + "]}";
//              + "], \"printTransformation\":true}";

      queryFile = FileUtility.createTemporary();
      Utility.copyStringToFile(queries_reg, queryFile);
      String expected_reg =
              "2 Q0 59 1 -1.73087481 galago" + newLine
              + "2 Q0 55 2 -1.73486418 galago" + newLine
              + "9 Q0 59 1 -1.38562925 galago" + newLine
              + "9 Q0 55 2 -1.38695903 galago" + newLine
              + "11 Q0 59 1 -2.07877996 galago" + newLine
              + "11 Q0 55 2 -2.08010975 galago" + newLine;
      runQueries(queryFile, expected_reg);
      
//  DEPRECATED - need to rethink a bit before re-enabling
//      String queries_back1 = "{ \"index\" : \"" + indexFile1.getAbsolutePath() + "\", \"queries\" : ["
//              + "{ \"number\" : \"2\", \"text\": \"#combine( #dirichlet( #counts:two:part=postings.krovetz() ) #dirichlet( #counts:sample:part=postings.krovetz() ) )\"},"
//              + "{ \"number\" : \"9\", \"text\" : \"#combine( #dirichlet( #counts:sample:part=postings.krovetz() ) )\"},"
//              + "{ \"number\" : \"11\", \"text\" : \"#combine( #dirichlet( #counts:is:part=postings.krovetz() ) #dirichlet( #counts:two:part=postings.krovetz() ) )\"},"
//              + "], \"backgroundPartMap\" : {\"postings.krovetz\" : \"background.krovetz\"}}";
//
//      Utility.copyStringToFile(queries_back1, queryFile);
//      runQueries(queryFile, expected_back);

      String queries_back2 = "{ \"index\" : {\"reg\" : \"" + JSONUtil.escape(indexFile1.getAbsolutePath()) + "\","
              + "\"back\" : \"" + JSONUtil.escape(backgroundIndex.getAbsolutePath()) + "\" }, "
              + "\"defaultGroup\" : \"reg\","
              + "\"queries\" : ["
              + " { \"number\" : \"2\", \"text\": \"#combine( #dirichlet( #counts:two:part=postings.krovetz() ) #dirichlet( #counts:sample:part=postings.krovetz() ) )\"},"
              + " { \"number\" : \"9\", \"text\" : \"#combine( #dirichlet( #counts:sample:part=postings.krovetz() ) )\"},"
              + " { \"number\" : \"11\", \"text\" : \"#combine( #dirichlet( #counts:is:part=postings.krovetz() ) #dirichlet( #counts:two:part=postings.krovetz() ) )\"},"
              + "],"
              + "\"backgroundIndex\" : \"back\"}";

      Utility.copyStringToFile(queries_back2, queryFile);

      String expected_back =
              "2 Q0 59 1 -1.60833350 galago" + newLine
              + "2 Q0 55 2 -1.61254386 galago" + newLine
              + "9 Q0 59 1 -1.06094589 galago" + newLine
              + "9 Q0 55 2 -1.06227568 galago" + newLine
              + "11 Q0 55 1 -2.36122993 galago" + newLine
              + "11 Q0 59 2 -2.36133423 galago" + newLine;
      
      runQueries(queryFile, expected_back);

    } finally {
      if (trecCorpusFile1 != null) {
        Assert.assertTrue(trecCorpusFile1.delete());
      }
      if (trecCorpusFile2 != null) {
        Assert.assertTrue(trecCorpusFile2.delete());
      }
      if (queryFile != null) {
        //TODO: jfoley - this intermittently fails on windows for me.
        //Assert.assertTrue(queryFile.delete());
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

  private static void runQueries(File queries, String expected) throws Exception {
    // Smoke test with batch search
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    App.run(new String[]{"batch-search",
        queries.getAbsolutePath()}, printStream);

    printStream.close();
    byteArrayStream.close();

    String output = byteArrayStream.toString();

    assertEquals(expected, output);
  }
}
