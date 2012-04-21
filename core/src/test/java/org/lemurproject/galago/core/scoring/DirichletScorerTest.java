// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.scoring;

import org.lemurproject.galago.core.scoring.DirichletScorer;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class DirichletScorerTest extends TestCase {

  public DirichletScorerTest(String testName) {
    super(testName);
  }

  public void testCollectionProbability() throws IOException {
    NodeParameters p = new NodeParameters();
    p.set("collectionProbability", 0.5);
    DirichletScorer scorer = new DirichletScorer(p, null);

    assertEquals(1500.0, scorer.mu);
    assertEquals(0.5, scorer.background);

    double score = scorer.score(15, 100);
    assertEquals(-0.73788, score, 0.001);
  }

  public void testSetMu() throws IOException {
    NodeParameters p = new NodeParameters();
    p.set("collectionProbability", 0.5);
    p.set("mu", 13);
    DirichletScorer scorer = new DirichletScorer(p, null);

    assertEquals(13.0, scorer.mu);
    assertEquals(0.5, scorer.background);

    double score = scorer.score(5, 100);
    assertEquals(-2.28504, score, 0.001);
  }
}
