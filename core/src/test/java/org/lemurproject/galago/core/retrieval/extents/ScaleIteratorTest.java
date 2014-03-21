// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.iterator.ScaleIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 *
 * @author marc
 */
public class ScaleIteratorTest {

  private final static int[] docsA = new int[]{5, 10, 15, 20};
  private final static double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
  private final static int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  private final static double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};

  @Test
  public void testA() throws Exception {
    ScoringContext context = new ScoringContext();

    FakeScoreIterator inner = new FakeScoreIterator(docsA, scoresA);
    ScaleIterator iterator = new ScaleIterator(new NodeParameters(), inner);
    assertFalse(iterator.isDone());
    assertTrue(iterator.hasMatch(docsA[0]));
    for (int i = 0; i < docsA.length; i++) {
      context.document = iterator.currentCandidate();
      assertEquals(docsA[i], iterator.currentCandidate());
      assertEquals(scoresA[i], iterator.score(context), 0.001);
      iterator.movePast(docsA[i]);
    }
    assertTrue(iterator.isDone());
    iterator.reset();
    assertTrue(iterator.hasMatch(docsA[0]));
  }

  @Test
  public void testB() throws Exception {
    ScoringContext context = new ScoringContext();
    int[] lengths = new int[docsB.length];
    Arrays.fill(lengths, 100);

    FakeScoreIterator inner = new FakeScoreIterator(docsB, scoresB);
    NodeParameters weightedParameters = new NodeParameters();
    weightedParameters.set("default", 0.5);
    ScaleIterator iterator = new ScaleIterator(weightedParameters, inner);
    assertFalse(iterator.isDone());
    assertTrue(iterator.hasMatch(docsB[0]));

    for (int i = 0; i < docsB.length; i++) {
      iterator.syncTo(docsB[i]);
      context.document = docsB[i];
      assertEquals(docsB[i], iterator.currentCandidate());
      assertEquals(scoresB[i] * 0.5, iterator.score(context), 0.001);
    }
    iterator.reset();
    assertTrue(iterator.hasMatch(docsB[0]));
  }
}
