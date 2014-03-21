// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class DirichletScorerTest {
  @Test
  public void testCollectionProbability() throws IOException {
    NodeParameters p = new NodeParameters();
    p.set("nodeFrequency", 1);
    p.set("collectionLength", 2);
    DirichletScorer scorer = new DirichletScorer(p);

    assertEquals(1500.0, scorer.mu, 0.001);
    assertEquals(0.5, scorer.background, 0.001);

    double score = scorer.score(15, 100);
    assertEquals(-0.73788, score, 0.001);
  }

  @Test
  public void testSetMu() throws IOException {
    NodeParameters p = new NodeParameters();
    p.set("nodeFrequency", 1);
    p.set("collectionLength", 2);
    p.set("mu", 13);
    DirichletScorer scorer = new DirichletScorer(p);

    assertEquals(13.0, scorer.mu, 0.001);
    assertEquals(0.5, scorer.background, 0.001);

    double score = scorer.score(5, 100);
    assertEquals(-2.28504, score, 0.001);
  }
}
