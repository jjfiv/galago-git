/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author sjh
 */
public class DumpTermStatisticsFnTest {

  File trecCorpusFile_1 = null;
  File trecCorpusFile_2 = null;
  File indexFile_1 = null;
  File indexFile_2 = null;
  File childPath_1 = null;
  File childPathStemmed_1 = null;
  File childPath_2 = null;
  File childPathStemmed_2 = null;

  @Before
  public void setUp() throws Exception {

    String trecCorpus = AppTest.trecDocument("doc1", "This is sample document one. <person>Michael</person> has a niece <person>Julia</person>")
            + AppTest.trecDocument("doc2", "This is sample document two. <person>Michael</person> has a niece <person>Claire</person>");
    trecCorpusFile_1 = FileUtility.createTemporary();
    StreamUtil.copyStringToFile(trecCorpus, trecCorpusFile_1);

    indexFile_1 = FileUtility.createTemporaryDirectory();

    // now, build an index from that
    App.main(new String[]{"build", "--indexPath=" + indexFile_1.getAbsolutePath(),
            "--inputPath=" + trecCorpusFile_1.getAbsolutePath(),
            "--tokenizer/fields+person"});

    // Checks path and components
    AppTest.verifyIndexStructures(indexFile_1);
    childPathStemmed_1 = new File(indexFile_1, "field.krovetz.person");
    assertTrue(childPathStemmed_1.exists());

    childPath_1 = new File(indexFile_1, "field.person");
    assertTrue(childPath_1.exists());

    // create a 2nd index
    trecCorpus = AppTest.trecDocument("doc10", "This is sample document ten. <person>Michael</person> had a cat named <person>Max</person>. <person>michael</person>");
    trecCorpusFile_2 = FileUtility.createTemporary();
    StreamUtil.copyStringToFile(trecCorpus, trecCorpusFile_2);

    indexFile_2 = FileUtility.createTemporaryDirectory();

    // now, build an index from that
    App.main(new String[]{"build", "--indexPath=" + indexFile_2.getAbsolutePath(),
            "--inputPath=" + trecCorpusFile_2.getAbsolutePath(),
            "--tokenizer/fields+person"});

    // Checks path and components
    AppTest.verifyIndexStructures(indexFile_2);
    childPathStemmed_2 = new File(indexFile_2, "field.krovetz.person");
    assertTrue(childPathStemmed_2.exists());

    childPath_2 = new File(indexFile_2, "field.person");
    assertTrue(childPath_2.exists());

  }

  @After
  public void tearDown() throws IOException {

    if (trecCorpusFile_1 != null) {
      trecCorpusFile_1.delete();
    }
    if (trecCorpusFile_2 != null) {
      trecCorpusFile_2.delete();
    }
    if (indexFile_1 != null) {
      FSUtil.deleteDirectory(indexFile_1);
    }
    if (indexFile_2 != null) {
      FSUtil.deleteDirectory(indexFile_2);
    }

  }

  @Test
  public void test() throws Exception {

    // test with a single index
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    // test non-stemmed
    App.run(new String[]{"dump-term-stats",
            childPath_1.getAbsolutePath(),}, printStream);

    String output = byteArrayStream.toString();
    // can't just do a string comparison beczuse we can't relly on the order.
    Set<String> items = new HashSet<String>(Arrays.asList(output.split("\n")));

    assertTrue(items.size() == 3);
    String expected = "michael\t2\t2\njulia\t1\t1\nclaire\t1\t1\n";
    Set<String> expectedSet = new HashSet<String>(Arrays.asList(expected.split("\n")));
    assertTrue(items.containsAll(expectedSet));

    // test stemmed
    byteArrayStream = new ByteArrayOutputStream();
    printStream = new PrintStream(byteArrayStream);
    App.run(new String[]{"dump-term-stats",
            childPathStemmed_1.getAbsolutePath(),}, printStream);

    output = byteArrayStream.toString();
    assertEquals(expected, output);

    // test with two indexes
    byteArrayStream = new ByteArrayOutputStream();
    printStream = new PrintStream(byteArrayStream);

    App.run(new String[]{"dump-term-stats",
            childPath_1.getAbsolutePath() + "," + childPath_2.getAbsolutePath(),}, printStream);

    output = byteArrayStream.toString();
    // can't just do a string comparison beczuse we can't relly on the order.
    items = new HashSet<String>(Arrays.asList(output.split("\n")));

    assertTrue(items.size() == 4);
    expected = "michael\t4\t3\nmax\t1\t1\njulia\t1\t1\nclaire\t1\t1\n";
    expectedSet = new HashSet<String>(Arrays.asList(expected.split("\n")));
    assertTrue(items.containsAll(expectedSet));


  }

  @Test
  public void testExtendedVersion() throws Exception {

      // test with two indexes
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(byteArrayStream);

    // test w/o min TF/DF
    App.run(new String[]{"dump-term-stats-ext",
            "--indexParts=" + childPath_1.getAbsolutePath() + "," + childPath_2.getAbsolutePath(),}, printStream);

    String output = byteArrayStream.toString();
    // can't just do a string comparison beczuse we can't relly on the order.
    Set<String> items = new HashSet<String>(Arrays.asList(output.split("\n")));

    assertTrue(items.size() == 4);
    String expected = "michael\t4\t3\nmax\t1\t1\njulia\t1\t1\nclaire\t1\t1\n";
    Set<String> expectedSet = new HashSet<String>(Arrays.asList(expected.split("\n")));
    assertTrue(items.containsAll(expectedSet));

    // test min TF
    byteArrayStream = new ByteArrayOutputStream();
    printStream = new PrintStream(byteArrayStream);

    App.run(new String[]{"dump-term-stats-ext",
            "--minTF=3", "--indexParts=" + childPath_1.getAbsolutePath() + "," + childPath_2.getAbsolutePath(),}, printStream);

    output = byteArrayStream.toString();
    // can't just do a string comparison beczuse we can't relly on the order.
    items = new HashSet<String>(Arrays.asList(output.split("\n")));

    assertTrue(items.size() == 1);
    expected = "michael\t4\t3\n";
    expectedSet = new HashSet<String>(Arrays.asList(expected.split("\n")));
    assertTrue(items.containsAll(expectedSet));

    // test min DF
    byteArrayStream = new ByteArrayOutputStream();
    printStream = new PrintStream(byteArrayStream);

    App.run(new String[]{"dump-term-stats-ext",
            "--minDF=3", "--indexParts=" + childPath_1.getAbsolutePath() + "," + childPath_2.getAbsolutePath(),}, printStream);

    output = byteArrayStream.toString();
    // can't just do a string comparison beczuse we can't relly on the order.
    items = new HashSet<String>(Arrays.asList(output.split("\n")));

    assertTrue(items.size() == 1);
    expected = "michael\t4\t3\n";
    expectedSet = new HashSet<String>(Arrays.asList(expected.split("\n")));
    assertTrue(items.containsAll(expectedSet));

    // test with in TF/DF
    byteArrayStream = new ByteArrayOutputStream();
    printStream = new PrintStream(byteArrayStream);

    App.run(new String[]{"dump-term-stats-ext",
            "--minTF=1", "--minDF=1", "--indexParts=" + childPath_1.getAbsolutePath() + "," + childPath_2.getAbsolutePath(),}, printStream);

    output = byteArrayStream.toString();
    items = new HashSet<String>(Arrays.asList(output.split("\n")));

    assertTrue(items.size() == 4);
    expected = "michael\t4\t3\nmax\t1\t1\njulia\t1\t1\nclaire\t1\t1\n";
    expectedSet = new HashSet<String>(Arrays.asList(expected.split("\n")));
    assertTrue(items.containsAll(expectedSet));


  }

}
