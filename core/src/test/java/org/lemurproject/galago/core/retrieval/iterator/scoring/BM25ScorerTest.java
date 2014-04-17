// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

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
}
