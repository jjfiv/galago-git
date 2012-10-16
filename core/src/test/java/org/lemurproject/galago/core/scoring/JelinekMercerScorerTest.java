// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.scoring;

import org.lemurproject.galago.core.scoring.JelinekMercerScorer;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class JelinekMercerScorerTest extends TestCase {

  public JelinekMercerScorerTest(String testName) {
    super(testName);
  }

  public void testCollectionProbability() throws IOException {
    NodeParameters p = new NodeParameters();
    p.set("nodeFrequency", 1);
    p.set("collectionLength", 2);
    JelinekMercerScorer scorer = new JelinekMercerScorer(p, null);

    assertEquals(0.5, scorer.lambda);
    assertEquals(0.5, scorer.background);

    double score = scorer.score(15, 100);
    assertEquals(-1.12393, score, 0.001);
  }

  public void testSetLambda() throws IOException {
    NodeParameters p = new NodeParameters();
    p.set("lambda", 0.2);
    p.set("nodeFrequency", 1);
    p.set("collectionLength", 2);
    JelinekMercerScorer scorer = new JelinekMercerScorer(p, null);

    assertEquals(0.2, scorer.lambda);
    assertEquals(0.5, scorer.background);

    double score = scorer.score(5, 100);
    assertEquals(-0.89160, score, 0.001);
  }
}
