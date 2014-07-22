/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class DeltaScoreDocumentModelTest {

  @Test
  public void testMaxscore() throws Exception {
    File corpus = FileUtility.createTemporary();
    File index = FileUtility.createTemporaryDirectory();
    try {
      makeIndex(corpus, index);

      Parameters globals = Parameters.instance();
      LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

      Parameters queryParams = Parameters.instance();
      queryParams.set("requested", 10);

      Node query = StructuredQuery.parse("#combine( test text 0 1 2 3 4 )");
      query = ret.transformQuery(query, queryParams);

      MaxScoreDocumentModel deltaModel = new MaxScoreDocumentModel(ret);
      ScoredDocument[] deltaResults = deltaModel.execute(query, queryParams);

      RankedDocumentModel safeModel = new RankedDocumentModel(ret);
      ScoredDocument[] safeResults = safeModel.execute(query, queryParams);

      assertEquals(safeResults.length, deltaResults.length);
      for (int i = 0; i < safeResults.length; ++i) {
        assertEquals(safeResults[i].document, deltaResults[i].document);
        assertEquals(safeResults[i].score, deltaResults[i].score, 0.00001);
      }

      // check that weights are correctly propagated
      queryParams.set("flattenCombine", false);
      Node complexQuery = StructuredQuery.parse("#combine:0=0.9:1=0.1(#combine( test text ) #combine( 1 2 ))");
      complexQuery = ret.transformQuery(complexQuery, queryParams);

      ScoredDocument[] deltaResults2 = deltaModel.execute(complexQuery, queryParams);
      ScoredDocument[] safeResults2 = safeModel.execute(complexQuery, queryParams);

      for (int i = 0; i < safeResults2.length; ++i) {
        assertEquals(safeResults2[i].document, deltaResults2[i].document);
        assertEquals(safeResults2[i].score, deltaResults2[i].score, 0.00001);
      }

    } finally {
      corpus.delete();
      FSUtil.deleteDirectory(index);
    }
  }

  @Test
  public void testWAND() throws Exception {
    File corpus = FileUtility.createTemporary();
    File index = FileUtility.createTemporaryDirectory();
    try {
      makeIndex(corpus, index);

      Parameters globals = Parameters.instance();
      LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

      Parameters queryParams = Parameters.instance();
      queryParams.set("requested", 10);

      Node query = StructuredQuery.parse("#combine( test text 0 1 2 3 4 90 )");
      query = ret.transformQuery(query, queryParams);

      WeakAndDocumentModel deltaModel = new WeakAndDocumentModel(ret);
      ScoredDocument[] deltaResults = deltaModel.execute(query, queryParams);

      RankedDocumentModel safeModel = new RankedDocumentModel(ret);
      ScoredDocument[] safeResults = safeModel.execute(query, queryParams);

      assertEquals(safeResults.length, deltaResults.length);
      for (int i = 0; i < safeResults.length; ++i) {
        assertEquals(safeResults[i].document, deltaResults[i].document);
        assertEquals(safeResults[i].score, deltaResults[i].score, 0.00001);
      }
    } finally {
      corpus.delete();
      FSUtil.deleteDirectory(index);
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

    Parameters p = Parameters.instance();
    p.set("inputPath", corpus.getAbsolutePath());
    p.set("indexPath", index.getAbsolutePath());
    App.run("build", p, System.out);
    System.out.println("build finished?");
  }
}
