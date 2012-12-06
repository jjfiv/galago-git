// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.structured;

import java.io.ByteArrayOutputStream;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import java.io.File;
import java.io.PrintStream;
import java.util.HashSet;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Tests the #require and #reject operators against stored date fields.
 * @author irmarc
 */
public class FilteringIteratorTest extends TestCase {

  public File tempPath;

  public FilteringIteratorTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws Exception {
    tempPath = LocalRetrievalTest.makeIndex();
  }

  @Override
  public void tearDown() throws Exception {
    Utility.deleteDirectory(tempPath);
  }

  public void testFilteredXCountOperator() throws Exception {
    File queryFile1 = null;
    try {
      // Run an Xcount over this particular query

      // try to batch search that index with a no-match string
      String q = "#require( #between( date 1/1/1900 1/1/2020 ) a )";
      String queries = "{ \"x\" : [\"" + q + "\"]}";

      queryFile1 = Utility.createTemporary();
      Utility.copyStringToFile(queries, queryFile1);


      // Smoke test with batch search
      ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
      PrintStream printStream = new PrintStream(byteArrayStream);

      new App().run(new String[]{"xcount",
                "--index=" + tempPath.getAbsolutePath(),
                queryFile1.getAbsolutePath()}, printStream);

      // Now, verify that we got the right count
      String output = byteArrayStream.toString().trim();
      String expected = "4\t" + q;

      assertEquals(expected, output);

    } finally {
      if (queryFile1 != null) {
        queryFile1.delete();
      }
    }
  }

  public void testRequireOperator() throws Exception {
    DiskIndex index = new DiskIndex(tempPath.getAbsolutePath());
    LocalRetrieval retrieval = new LocalRetrieval(index, new Parameters());

    // Let's try requiring a greater than
    Node tree = StructuredQuery.parse("#combine ( #require( #greater( date 1/7/1920 ) a ) #require( #greater( date 1/7/1920 ) b ) ) ");

    Parameters qp = new Parameters();
    tree = retrieval.transformQuery(tree, qp);
    ScoredDocument[] results = retrieval.runQuery(tree, qp);
    assertEquals(3, results.length);

    // Check each doc - don't really care about order
    HashSet<Integer> set = new HashSet<Integer>();
    set.add(1);
    set.add(2);
    set.add(5);
    for (ScoredDocument sd : results) {
      assertTrue(set.contains(sd.document));
    }
  }

  public void testRejectOperator() throws Exception {
    DiskIndex index = new DiskIndex(tempPath.getAbsolutePath());
    LocalRetrieval retrieval = new LocalRetrieval(index, new Parameters());

    // Let's try requiring a greater than
    Node tree = StructuredQuery.parse("#combine( #reject( #between( date 1/1/0100 1/1/1900 ) b ) #reject( #between( date 1/1/0100 1/1/1900)a ) )");
    Parameters qp = new Parameters();
    tree = retrieval.transformQuery(tree, qp);

    ScoredDocument[] results = retrieval.runQuery(tree, qp);
    assertEquals(3, results.length);

    // Check each doc - don't really care about order
    HashSet<Integer> set = new HashSet<Integer>();
    set.add(1);
    set.add(2);
    set.add(5);
    for (ScoredDocument sd : results) {
      assertTrue(set.contains(sd.document));
    }
  }
}
