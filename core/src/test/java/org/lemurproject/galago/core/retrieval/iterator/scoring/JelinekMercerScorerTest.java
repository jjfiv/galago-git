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
public class JelinekMercerScorerTest {

  @Test
  public void testCollectionProbability() throws IOException {
    NodeParameters p = new NodeParameters();
    p.set("nodeFrequency", 1);
    p.set("collectionLength", 2);
    JelinekMercerScorer scorer = new JelinekMercerScorer(p);


    assertEquals(0.5, scorer.lambda, 0.001);
    assertEquals(0.5, scorer.background, 0.001);

    double score = scorer.score(15, 100);
    assertEquals(-1.12393, score, 0.001);
  }

  @Test
  public void testSetLambda() throws IOException {
    NodeParameters p = new NodeParameters();
    p.set("lambda", 0.2);
    p.set("nodeFrequency", 1);
    p.set("collectionLength", 2);
    JelinekMercerScorer scorer = new JelinekMercerScorer(p);

    assertEquals(0.2, scorer.lambda, 0.001);
    assertEquals(0.5, scorer.background, 0.001);

    double score = scorer.score(5, 100);
    assertEquals(-0.89160, score, 0.001);
  }
}
