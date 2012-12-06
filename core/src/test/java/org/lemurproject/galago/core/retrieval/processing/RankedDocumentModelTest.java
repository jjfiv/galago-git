/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.io.File;
import java.util.Arrays;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class RankedDocumentModelTest extends TestCase {

  File corpus = null;
  File index = null;

  public RankedDocumentModelTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() {
    try {
      corpus = Utility.createTemporary();
      index = Utility.createTemporaryDirectory();
      makeIndex(corpus, index);
    } catch (Exception e) {
      tearDown();
    }
  }

  @Override
  public void tearDown() {
    try {
      if (corpus != null) {
        corpus.delete();
      }
      if (index != null) {
        Utility.deleteDirectory(index);
      }
    } catch (Exception e) {
    }
  }

  public void testEntireCollection() throws Exception {
    Parameters globals = new Parameters();
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

    Parameters queryParams = new Parameters();
    queryParams.set("requested", 10);

    Node query = StructuredQuery.parse("#combine( test text 0 1 2 3 4 )");
    query = ret.transformQuery(query, queryParams);

    RankedDocumentModel model = new RankedDocumentModel(ret);
    ScoredDocument[] results = model.execute(query, queryParams);

    assertEquals(results.length, 10);
    for (int i = 0; i < 10; i++) {
      assertEquals(results[i].document, i);
      assertEquals(results[i].rank, i + 1);
      if (i > 0) {
        assert (Utility.compare(results[i].score, results[i - 1].score) <= 0);
      }
    }

    query = StructuredQuery.parse("#combine( test text 99 )");
    query = ret.transformQuery(query, queryParams);

    results = model.execute(query, queryParams);

    assertEquals(results.length, 10);
    for (int i = 0; i < 10; i++) {
      assertEquals(results[i].document, i + 90);
      assertEquals(results[i].rank, i + 1);
      if (i > 0) {
        assert (Utility.compare(results[i].score, results[i - 1].score) <= 0);
      }
    }
  }

  public void testWhiteList() throws Exception {
    Parameters globals = new Parameters();
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

    Parameters queryParams = new Parameters();
    queryParams.set("requested", 10);

    Node query = StructuredQuery.parse("#combine( test text 0 1 2 3 4 )");
    query = ret.transformQuery(query, queryParams);

    WorkingSetDocumentModel model = new WorkingSetDocumentModel(ret);
    queryParams.set("working", 
            Arrays.asList(new Integer[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 13, 14}));
    ScoredDocument[] results = model.execute(query, queryParams);

    assertEquals(results.length, 10);
    for (int i = 0; i < 9; i++) {
      assertEquals(results[i].document, i);
      assertEquals(results[i].rank, i + 1);
      if (i > 0) {
        assert (Utility.compare(results[i].score, results[i - 1].score) <= 0);
      }
    }
    // document 10 is not in the white list:      
    assertEquals(results[9].document, 11);
    assertEquals(results[9].rank, 10);
    assert (Utility.compare(results[9].score, results[8].score) <= 0);



    query = StructuredQuery.parse("#combine( test text 90 )");
    query = ret.transformQuery(query, queryParams);

    queryParams.set("working", 
            Arrays.asList(new Integer[]{0, 1, 2, 3, 4, 5, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99}));

    results = model.execute(query, queryParams);

    assertEquals(results.length, 10);
    for (int i = 0; i < 10; i++) {
      assertEquals(results[i].document, i + 90);
      assertEquals(results[i].rank, i + 1);
      if (i > 0) {
        assert (Utility.compare(results[i].score, results[i - 1].score) <= 0);
      }
    }
  }

  private void makeIndex(File corpus, File index) throws Exception {
    StringBuilder c = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      StringBuilder data = new StringBuilder();
      for (int j = 0; j < (i + 10); j++) {
        data.append(" ").append(j);
      }
      c.append(AppTest.trecDocument("d-" + i, "Test text" + data.toString()));
    }
    Utility.copyStringToFile(c.toString(), corpus);

    Parameters p = new Parameters();
    p.set("inputPath", corpus.getAbsolutePath());
    p.set("indexPath", index.getAbsolutePath());
    App.run("build", p, System.out);
  }
}
