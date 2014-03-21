/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class RankedPassageModelTest {

  static File corpus = null;
  static File index = null;

  @BeforeClass
  public static void setUp() throws Exception {
    corpus = FileUtility.createTemporary();
    index = FileUtility.createTemporaryDirectory();
    makeIndex(corpus, index);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (corpus != null) {
      corpus.delete();
    }
    if (index != null) {
      Utility.deleteDirectory(index);
    }
  }

  @Test
  public void testEntireCollection() throws Exception {
    Parameters globals = new Parameters();
    globals.set("passageQuery", true);
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

    Parameters queryParams = new Parameters();
    int req = 10;
    queryParams.set("requested", req);
    queryParams.set("passageQuery", true);
    queryParams.set("passageSize", 10);
    queryParams.set("passageShift", 5);

    Node query = StructuredQuery.parse("#combine( test text 0 1 )");
    query = ret.transformQuery(query, queryParams);

    RankedPassageModel model = new RankedPassageModel(ret);

    ScoredPassage[] results = (ScoredPassage[]) model.execute(query, queryParams);

    // --- all documents contain these terms in the first ten words --
    // -> this query should only ever return the first passage (0-10)
    // -> and the top 100 scores should be equal
    assertEquals(results.length, req);
    for (int i = 0; i < req; i++) {
      assertEquals(results[i].document, i);
      assertEquals(results[i].begin, 0);
      assertEquals(results[i].end, 10);
      assertEquals(results[i].rank, i + 1);
      if (i > 0) {
        assert (Utility.compare(results[i].score, results[i - 1].score) == 0);
      }
    }

    query = StructuredQuery.parse("#combine( test text 99 )");
    query = ret.transformQuery(query, queryParams);

    results = (ScoredPassage[]) model.execute(query, queryParams);

    // note that dirichlet favours smaller documents over longer documents 

    assertEquals(results.length, req);
    assertEquals(results[0].document, 94);
    assertEquals(results[0].begin, 100);
    assertEquals(results[0].end, 106);
    assertEquals(results[0].rank, 1);
    assertEquals(results[0].score, -4.776027, 0.000001);

    assertEquals(results[1].document, 90);
    assertEquals(results[1].begin, 95);
    assertEquals(results[1].end, 102);
    assertEquals(results[1].rank, 2);
    assertEquals(results[1].score, -4.776691, 0.000001);

    assertEquals(results[2].document, 95);
    assertEquals(results[2].begin, 100);
    assertEquals(results[2].end, 107);
    assertEquals(results[2].rank, 3);
    assertEquals(results[2].score, -4.776691, 0.000001);

  }

  private ScoredPassage[] scorePassages(Parameters p) throws Exception {
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath());
    Node query = StructuredQuery.parse("test");
    query = ret.transformQuery(query, p);
    WorkingSetPassageModel model = new WorkingSetPassageModel(ret);
    return (ScoredPassage[]) model.execute(query, p);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingPassageQuery() throws Exception {
    Parameters badParms = new Parameters();
    badParms.set("requested", 100);
    badParms.set("passageSize", 10);
    badParms.set("passageShift", 5);
    badParms.set("working",
      Arrays.asList(new Long[]{2l}));
    scorePassages(badParms);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingPassageSize() throws Exception {
    Parameters badParms = new Parameters();
    badParms.set("passageQuery", true);
    badParms.set("passageShift", 5);
    badParms.set("working",
      Arrays.asList(new Long[]{2l}));
    scorePassages(badParms);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingPassageShift() throws Exception {
    Parameters badParms = new Parameters();
    badParms.set("passageQuery", true);
    badParms.set("passageSize", 10);
    badParms.set("working",
      Arrays.asList(new Long[]{2l}));
    scorePassages(badParms);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingWorkingSet() throws Exception {
    Parameters badParms = new Parameters();
    badParms.set("passageQuery", true);
    badParms.set("passageSize", 10);
    badParms.set("passageShift", 5);
    scorePassages(badParms);
  }

  @Test
  public void testWhiteList() throws Exception {
    Parameters globals = new Parameters();
    globals.set("passageQuery", true);
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

    Parameters queryParams = new Parameters();
    queryParams.set("requested", 100);
    queryParams.set("passageQuery", true);
    queryParams.set("passageSize", 10);
    queryParams.set("passageShift", 5);

    Node query = StructuredQuery.parse("#combine( test text 0 1 )");
    query = ret.transformQuery(query, queryParams);

    WorkingSetPassageModel model = new WorkingSetPassageModel(ret);
    queryParams.set("working",
            Arrays.asList(new Long[]{2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l}));

    ScoredPassage[] results = (ScoredPassage[]) model.execute(query, queryParams);

    // --- all documents contain these terms in the first ten words --
    // -> this query should only ever return the first passage (0-10)
    // -> and all scores should be equal
     assertEquals(31, results.length);
    for (int i = 0; i < 10; i++) {
      assertEquals(results[i].document, i + 2);
      assertEquals(results[i].begin, 0);
      assertEquals(results[i].end, 10);
      assertEquals(results[i].rank, i + 1);
      assertEquals(results[i].score, -4.085500, 0.000001);
    }

    query = StructuredQuery.parse("#combine( test text 80 )");
    query = ret.transformQuery(query, queryParams);

    queryParams.set("working",
            Arrays.asList(new Long[]{0l, 1l, 2l, 3l, 4l, 89l, 90l, 91l, 92l, 93l}));
    results = (ScoredPassage[]) model.execute(query, queryParams);

    assertEquals(results.length, 100);
    
    // higher documents, with the term '89', 
    // are ranked highest because 'test' and 'text' exist in every document (~= stopwords)

    assertEquals(results[0].document, 89);
    assertEquals(results[0].begin, 75);
    assertEquals(results[0].end, 85);
    assertEquals(results[0].rank, 1);
    assertEquals(results[0].score, -4.49422735, 0.000001);

    assertEquals(results[1].document, 89);
    assertEquals(results[1].begin, 80);
    assertEquals(results[1].end, 90);
    assertEquals(results[1].rank, 2);
    assertEquals(results[1].score, -4.49422735, 0.000001);

    assertEquals(results[10].document, 0);
    assertEquals(results[10].begin, 0);
    assertEquals(results[10].end, 10);
    assertEquals(results[10].rank, 11);
    assertEquals(results[10].score, -4.51151864, 0.000001);

    assertEquals(results[15].document, 89);
    assertEquals(results[15].begin, 0);
    assertEquals(results[15].end, 10);
    assertEquals(results[15].rank, 16);
    assertEquals(results[15].score, -4.51151864, 0.000001);
    
    // 

  }

  public static void makeIndex(File corpus, File index) throws Exception {
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
    p.set("corpus", false);
    App.run("build", p, System.out);
  }
}
