/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class RankedDocumentModelTest {

  File corpus = null;
  File index = null;

  @Before
  public void setUp() throws Exception {
    corpus = FileUtility.createTemporary();
    index = FileUtility.createTemporaryDirectory();
    makeIndex(corpus, index);
  }

  @After
  public void tearDown() throws IOException {
    if (corpus != null) {
      corpus.delete();
    }
    if (index != null) {
      FSUtil.deleteDirectory(index);
    }
  }

  @Test
  public void testEntireCollection() throws Exception {
    Parameters globals = Parameters.create();
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

    Parameters queryParams = Parameters.create();
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
        assert (CmpUtil.compare(results[i].score, results[i - 1].score) <= 0);
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
        assert (CmpUtil.compare(results[i].score, results[i - 1].score) <= 0);
      }
    }
  }

  @Test
  public void testWhiteList() throws Exception {
    Parameters globals = Parameters.create();
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

    Parameters queryParams = Parameters.create();
    queryParams.set("requested", 10);

    Node query = StructuredQuery.parse("#combine( test text 0 1 2 3 4 )");
    query = ret.transformQuery(query, queryParams);

    WorkingSetDocumentModel model = new WorkingSetDocumentModel(ret);
    queryParams.set("working", 
            Arrays.asList(new Long[]{0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 11L, 12L, 13L, 14L}));
    ScoredDocument[] results = model.execute(query, queryParams);

    assertEquals(results.length, 10);
    for (int i = 0; i < 9; i++) {
      assertEquals(results[i].document, i);
      assertEquals(results[i].rank, i + 1);
      if (i > 0) {
        assert (CmpUtil.compare(results[i].score, results[i - 1].score) <= 0);
      }
    }
    // document 10 is not in the white list:      
    assertEquals(results[9].document, 11);
    assertEquals(results[9].rank, 10);
    assert (CmpUtil.compare(results[9].score, results[8].score) <= 0);



    query = StructuredQuery.parse("#combine( test text 90 )");
    query = ret.transformQuery(query, queryParams);

    queryParams.set("working",
            Arrays.asList(new Long[]{0L, 1L, 2L, 3L, 4L, 5L, 90L, 91L, 92L, 93L, 94L, 95L, 96L, 97L, 98L, 99L}));

    results = model.execute(query, queryParams);

    assertEquals(results.length, 10);
    for (int i = 0; i < 10; i++) {
      assertEquals(results[i].document, i + 90);
      assertEquals(results[i].rank, i + 1);
      if (i > 0) {
        assert (CmpUtil.compare(results[i].score, results[i - 1].score) <= 0);
      }
    }

    // test that external doc IDs can be used in working set
    queryParams.set("working",
            Arrays.asList(new String[]{"d-0", "d-1", "d-2", "d-3", "d-4", "d-5", "d-90", "d-91", "d-92", "d-93", "d-94", "d-95", "d-96", "d-97", "d-98", "d-99"}));

    results = model.execute(query, queryParams);

    assertEquals(results.length, 10);
    for (int i = 0; i < 10; i++) {
      assertEquals(results[i].document, i + 90);
      assertEquals(results[i].rank, i + 1);
      if (i > 0) {
        assert (CmpUtil.compare(results[i].score, results[i - 1].score) <= 0);
      }
    }
  }

  private static void makeIndex(File corpus, File index) throws Exception {
    StringBuilder c = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      StringBuilder data = new StringBuilder();
      for (int j = 0; j < (i + 10); j++) {
        data.append(" ").append(j);
      }
      c.append(AppTest.trecDocument("d-" + i, "Test text" + data.toString()));
    }
    StreamUtil.copyStringToFile(c.toString(), corpus);

    Parameters p = Parameters.create();
    p.set("inputPath", corpus.getAbsolutePath());
    p.set("indexPath", index.getAbsolutePath());
    App.run("build", p, System.out);
  }
}
