// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.iterator.ScoreCombinationIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor
 */
public class UnfilteredCombinationIteratorTest {

  private static final int[] docsA = new int[]{5, 10, 15, 20};
  private static final double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
  private static final int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  private static final double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  private static final int[] docsTogether = new int[]{2, 4, 5, 6, 8, 10, 12, 14, 15, 16, 18, 20};
  private static final double[] scoresTogether = new double[]{1, 2, 0.5, 3, 4, 6, 6, 7, 1.5, 8, 9, 12};

  @Test
  public void testNextCandidateAny() throws IOException {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    ScoreCombinationIterator instance = new ScoreCombinationIterator(new NodeParameters(),
            iterators);

    assertEquals(2, instance.currentCandidate());
    instance.movePast(2);
    assertEquals(4, instance.currentCandidate());
    instance.movePast(4);
    assertEquals(5, instance.currentCandidate());
    instance.movePast(5);
    assertEquals(6, instance.currentCandidate());
  }

  @Test
  public void testHasMatch() {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};
    ScoreCombinationIterator instance = new ScoreCombinationIterator(new NodeParameters(),
            iterators);

    assertFalse(instance.hasMatch(1));
    assertTrue(instance.hasMatch(2));
    assertFalse(instance.hasMatch(3));
  }

  @Test
  public void testScore() throws IOException {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    ScoreCombinationIterator instance = new ScoreCombinationIterator(new NodeParameters(),
            iterators);

    ScoringContext context = new ScoringContext();

    int[] lengths = new int[12];
    Arrays.fill(lengths, 100);
    for (int i = 0; i < 12; i++) {
      assertFalse(instance.isDone());
      assertTrue(instance.hasMatch(docsTogether[i]));
      context.document = docsTogether[i];
      assertEquals(scoresTogether[i], instance.score(context), 0.0000001);

      instance.movePast(docsTogether[i]);
    }

    assertTrue(instance.isDone());
  }

  @Test
  public void testMovePast() throws Exception {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    ScoreCombinationIterator instance = new ScoreCombinationIterator(new NodeParameters(),
            iterators);

    instance.movePast(5);
    assertEquals(6, instance.currentCandidate());
  }

  @Test
  public void testMoveTo() throws Exception {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    ScoreCombinationIterator instance = new ScoreCombinationIterator(new NodeParameters(),
            iterators);

    instance.syncTo(5);
    assertEquals(5, instance.currentCandidate());
  }
}
