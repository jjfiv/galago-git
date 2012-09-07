// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import java.util.Arrays;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.FakeLengthIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ScaleIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author marc
 */
public class ScaleIteratorTest extends TestCase {

  int[] docsA = new int[]{5, 10, 15, 20};
  double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
  int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};

  public ScaleIteratorTest(String testName) {
    super(testName);
  }

  public void testA() throws Exception {
    ScoringContext context = new ScoringContext();
    int[] lengths = new int[docsA.length];
    Arrays.fill(lengths, 100);
    FakeLengthIterator fli = new FakeLengthIterator(docsA, lengths);
    context.addLength("", fli);

    FakeScoreIterator inner = new FakeScoreIterator(docsA, scoresA);
    inner.setContext(context);
    ScaleIterator iterator = new ScaleIterator(new NodeParameters(), inner);
    assertFalse(iterator.isDone());
    assertTrue(iterator.hasMatch(docsA[0]));
    for (int i = 0; i < docsA.length; i++) {
      context.moveLengths(iterator.currentCandidate());
      context.document = iterator.currentCandidate();
      assertEquals(docsA[i], iterator.currentCandidate());
      assertEquals(scoresA[i], iterator.score());
      iterator.movePast(docsA[i]);
    }
    assertTrue(iterator.isDone());
    iterator.reset();
    assertTrue(iterator.hasMatch(docsA[0]));
  }

  public void testB() throws Exception {
    ScoringContext context = new ScoringContext();
    int[] lengths = new int[docsB.length];
    Arrays.fill(lengths, 100);
    FakeLengthIterator fli = new FakeLengthIterator(docsB, lengths);
    context.addLength("", fli);

    FakeScoreIterator inner = new FakeScoreIterator(docsB, scoresB);
    inner.setContext(context);
    NodeParameters weightedParameters = new NodeParameters();
    weightedParameters.set("default", 0.5);
    ScaleIterator iterator = new ScaleIterator(weightedParameters, inner);
    assertFalse(iterator.isDone());
    assertTrue(iterator.hasMatch(docsB[0]));

    for (int i = 0; i < docsB.length; i++) {
      iterator.syncTo(docsB[i]);
      context.document = docsB[i];
      context.moveLengths(docsB[i]);
      assertEquals(docsB[i], iterator.currentCandidate());
      assertEquals(scoresB[i] * 0.5, iterator.score());
    }
    iterator.reset();
    assertTrue(iterator.hasMatch(docsB[0]));
  }
}
