/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
 
public class FieldRetrievalTest   {

 final String newLine = System.getProperty("line.separator");
 
  @Test
  public void testFieldRetrievalPipeline() throws Exception {
    File trecCorpusFile = null;
    File indexFile = null;
    File queryFile = null;

    try {
      String trecCorpus = AppTest.trecDocument("55", "<title>sampled sample</title> <other>This is a sample document</other>")
              + AppTest.trecDocument("59", "<title>another sample</title> <other>This is another document</other>");
      trecCorpusFile = FileUtility.createTemporary();
      Utility.copyStringToFile(trecCorpus, trecCorpusFile);

      indexFile = FileUtility.createTemporaryDirectory();
      // now, try to build an index from that
      App.main(new String[]{"build", "--indexPath=" + indexFile.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile.getAbsolutePath(),
                "--tokenizer/fields+title", "--tokenizer/fields+other"});

      // Checks path and components
      AppTest.verifyIndexStructures(indexFile);

      // field postings -- there are two files
      File childPath = new File(indexFile, "field.krovetz.title");
      assertTrue(childPath.exists());

      childPath = new File(indexFile, "field.krovetz.other");
      assertTrue(childPath.exists());

      childPath = new File(indexFile, "field.title");
      assertTrue(childPath.exists());

      childPath = new File(indexFile, "field.other");
      assertTrue(childPath.exists());

      // try to batch search that index with a no-match string
      String queries = "{ \"queries\" : ["
              + "{ \"number\" : \"1\", \"text\" : \"sample\"}," + newLine
              + "{ \"number\" : \"2\", \"text\" : \"#combine(#extents:sample:part=field.krovetz.title())\"}," + newLine
              + "{ \"number\" : \"3\", \"text\" : \"#combine(#inside(sample #field:title()))\"}," + newLine
              + "{ \"number\" : \"4\", \"text\" : \"#combine(#inside:noOpt=true(#extents:sample:part=postings() #field:title()))\"}," + newLine
              + "{ \"number\" : \"5\", \"text\" : \"#combine(#inside(#extents:sample:part=postings() #field:title()))\"}" + newLine
              //+ "], \"printTransformation\" : true}";
              +"]}" + newLine;
              
      queryFile = FileUtility.createTemporary();
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
              "1 Q0 55 1 -1.17683184 galago" + newLine
              + "1 Q0 59 2 -1.18048269 galago" + newLine
              + "2 Q0 55 1 -1.46523173 galago" + newLine
              + "2 Q0 59 2 -1.46744437 galago" + newLine
              + "3 Q0 55 1 -1.46523173 galago" + newLine
              + "3 Q0 59 2 -1.46744437 galago" + newLine
              + "4 Q0 59 1 -1.87147023 galago" + newLine
              + "4 Q0 55 2 -1.87213402 galago" + newLine
              + "5 Q0 59 1 -1.87147023 galago" + newLine
              + "5 Q0 55 2 -1.87213402 galago" + newLine;
        
//              "1 Q0 55 1 -1.17683184 galago\n"
//              + "1 Q0 59 2 -1.18048269 galago\n"
//              + "2 Q0 55 1 -0.29056168 galago\n"
//              + "2 Q0 59 2 -0.29078560 galago\n"
//              + "3 Q0 55 1 -0.29056168 galago\n"
//              + "3 Q0 59 2 -0.29078560 galago\n"
//              + "4 Q0 59 1 -1.87147023 galago\n"
//              + "4 Q0 55 2 -1.87213402 galago\n"
//              + "5 Q0 59 1 -0.69580676 galago\n"
//              + "5 Q0 55 2 -0.69647055 galago\n";
      
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
