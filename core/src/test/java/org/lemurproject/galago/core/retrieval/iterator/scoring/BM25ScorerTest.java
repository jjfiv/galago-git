// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.Results;
import org.lemurproject.galago.core.retrieval.processing.MaxScoreDocumentModel;
import org.lemurproject.galago.core.retrieval.processing.RankedDocumentModel;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author irmarc
 */
public class BM25ScorerTest {

  @Test
  public void testScorer() throws Exception {
    // start with as many defaults as possible and
    // a fake iterator
    NodeParameters p = new NodeParameters();
    p.set("collectionLength", 5000);
    p.set("documentCount", 100);
    p.set("nodeDocumentCount", 0);

    BM25Scorer scorer = new BM25Scorer(p);
    assertEquals(0.75, scorer.b, 0.001);
    assertEquals(1.2, scorer.k, 0.001);
    assertEquals(50.0, scorer.avgDocLength, 0.001);
    assertEquals(5.29832, scorer.idf, 0.0001);
    assertEquals(8.20866, scorer.score(5, 100), 0.0001);

    // Add in an iterator w/ some docs
    p.set("nodeDocumentCount", 5);
    scorer = new BM25Scorer(p);
    assertEquals(0.75, scorer.b, 0.001);
    assertEquals(1.2, scorer.k, 0.001);
    assertEquals(50.0, scorer.avgDocLength, 0.001);
    assertEquals(2.90042, scorer.idf, 0.0001);
    assertEquals(5.53660, scorer.score(12, 85), 0.0001);

    // explicitly set everything
    p.set("b", 0.3);
    p.set("k", 2.0);
    p.set("nodeDocumentCount", 20);
    scorer = new BM25Scorer(p);
    assertEquals(0.3, scorer.b, 0.001);
    assertEquals(2.0, scorer.k, 0.001);
    assertEquals(50.0, scorer.avgDocLength, 0.001);
    assertEquals(1.58474, scorer.idf, 0.0001);
    assertEquals(3.79327, scorer.score(15, 200), 0.0001);
  }

  /**
   * This test tests bm25 scores against past performance under both ranked document and maxscore models.
   * @throws Exception
   */
  @Test
  public void testActualScoring() throws Exception {
    File[] files = LocalRetrievalTest.make10DocIndex();
    File trecCorpusFile = files[0];
    File indexFile = files[2];

    for (String processingModel : Arrays.asList("rankeddocument", "maxscore")) {
      LocalRetrieval loc = new LocalRetrieval(indexFile.getAbsolutePath());
      Parameters qp = Parameters.create();
      qp.put("scorer", "bm25");
      qp.put("processingModel", processingModel);
      Node xbm25 = loc.transformQuery(StructuredQuery.parse("#combine(the dog is dumb)"), qp);

      assertEquals("#combine:w=1.0( " +
              "#bm25:collectionLength=70:documentCount=10:maximumCount=2:nodeDocumentCount=2:nodeFrequency=3:w=0.25( #lengths:document:part=lengths() #counts:the:part=postings() ) " +
              "#bm25:collectionLength=70:documentCount=10:maximumCount=0:nodeDocumentCount=0:nodeFrequency=0:w=0.25( #lengths:document:part=lengths() #counts:dog:part=postings() ) " +
              "#bm25:collectionLength=70:documentCount=10:maximumCount=1:nodeDocumentCount=3:nodeFrequency=3:w=0.25( #lengths:document:part=lengths() #counts:is:part=postings() ) " +
              "#bm25:collectionLength=70:documentCount=10:maximumCount=0:nodeDocumentCount=0:nodeFrequency=0:w=0.25( #lengths:document:part=lengths() #counts:dumb:part=postings() )" +
              " )",
          xbm25.toString());

      Results results = loc.executeQuery(xbm25, qp);
      switch (processingModel) {
        case "rankeddocument":
          assertEquals(RankedDocumentModel.class, results.processingModel);
          break;
        case "maxscore":
          assertEquals(MaxScoreDocumentModel.class, results.processingModel);
          break;
        default: throw new AssertionError("Bad processing model:"+processingModel);
      }
      Map<String, Double> actualScores = results.asDocumentFeatures();

      // Scores valid as of Nov. 10, 2015
      assertEquals(0.29719, actualScores.get("1"), 0.0001);
      assertEquals(0.49649, actualScores.get("2"), 0.0001);
      assertEquals(0.36809, actualScores.get("3"), 0.0001);
      assertEquals(0.21273, actualScores.get("5"), 0.0001);
      assertEquals(0.22330, actualScores.get("6"), 0.0001);
    }

    trecCorpusFile.delete();
    FSUtil.deleteDirectory(indexFile);
  }
}
