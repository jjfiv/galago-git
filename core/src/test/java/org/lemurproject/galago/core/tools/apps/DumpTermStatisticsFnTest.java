/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.junit.Test;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class DumpTermStatisticsFnTest {

  @Test
  public void test() throws Exception {
    File trecCorpusFile = null;
    File indexFile_1 = null;
    File indexFile_2 = null;

    try {
      String trecCorpus = AppTest.trecDocument("doc1", "This is sample document one. <person>Michael</person> has a niece <person>Julia</person>")
              + AppTest.trecDocument("doc2", "This is sample document two. <person>Michael</person> has a niece <person>Claire</person>");
      trecCorpusFile = FileUtility.createTemporary();
      StreamUtil.copyStringToFile(trecCorpus, trecCorpusFile);

      indexFile_1 = FileUtility.createTemporaryDirectory();

      // now, build an index from that
      App.main(new String[]{"build", "--indexPath=" + indexFile_1.getAbsolutePath(),
              "--inputPath=" + trecCorpusFile.getAbsolutePath(),
              "--tokenizer/fields+person"});

      // Checks path and components
      AppTest.verifyIndexStructures(indexFile_1);
      File childPathStemmed_1 = new File(indexFile_1, "field.krovetz.person");
      assertTrue(childPathStemmed_1.exists());

      File childPath_1 = new File(indexFile_1, "field.person");
      assertTrue(childPath_1.exists());


      // test with a single index
      ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
      PrintStream printStream = new PrintStream(byteArrayStream);

      // test non-stemmed
      App.run(new String[]{"dump-term-stats",
              childPath_1.getAbsolutePath(),}, printStream);

      String output = byteArrayStream.toString();
      String expected ="michael\t2\t2\njulia\t1\t1\nclaire\t1\t1\n";
      assertEquals(expected, output);

      // test stemmed
      byteArrayStream = new ByteArrayOutputStream();
      printStream = new PrintStream(byteArrayStream);
      App.run(new String[]{"dump-term-stats",
              childPathStemmed_1.getAbsolutePath(),}, printStream);

      output = byteArrayStream.toString();
      assertEquals(expected, output);

      // test with more than one index
      trecCorpus = AppTest.trecDocument("doc10", "This is sample document ten. <person>Michael</person> had a cat named <person>Max</person>. <person>michael</person>");
      trecCorpusFile = FileUtility.createTemporary();
      StreamUtil.copyStringToFile(trecCorpus, trecCorpusFile);

      indexFile_2 = FileUtility.createTemporaryDirectory();

      // now, build an index from that
      App.main(new String[]{"build", "--indexPath=" + indexFile_2.getAbsolutePath(),
              "--inputPath=" + trecCorpusFile.getAbsolutePath(),
              "--tokenizer/fields+person"});

      // Checks path and components
      AppTest.verifyIndexStructures(indexFile_1);
      File childPathStemmed_2 = new File(indexFile_2, "field.krovetz.person");
      assertTrue(childPathStemmed_2.exists());

      File childPath_2 = new File(indexFile_2, "field.person");
      assertTrue(childPath_2.exists());

      // test with two indexes
      byteArrayStream = new ByteArrayOutputStream();
      printStream = new PrintStream(byteArrayStream);

      App.run(new String[]{"dump-term-stats",
              childPath_1.getAbsolutePath() + "," + childPath_2.getAbsolutePath(),}, printStream);

      output = byteArrayStream.toString();
      expected ="michael\t4\t3\njulia\t1\t1\nmax\t1\t1\nclaire\t1\t1\n";
      assertEquals(expected, output);

    } finally {
      if (trecCorpusFile != null) {
        trecCorpusFile.delete();
      }
      if (indexFile_1 != null) {
        FSUtil.deleteDirectory(indexFile_1);
      }
      if (indexFile_2 != null) {
        FSUtil.deleteDirectory(indexFile_2);
      }
    }
  }

}
