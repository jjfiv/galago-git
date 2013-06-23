// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.retrieval.iterator.ScoreCombinationIterator;
import java.io.IOException;
import java.util.Arrays;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.FakeLengthIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class UnfilteredCombinationIteratorTest extends TestCase {

  public UnfilteredCombinationIteratorTest(String testName) {
    super(testName);
  }
  int[] docsA = new int[]{5, 10, 15, 20};
  double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
  int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  int[] docsTogether = new int[]{2, 4, 5, 6, 8, 10, 12, 14, 15, 16, 18, 20};
  double[] scoresTogether = new double[]{1, 2, 0.5, 3, 4, 6, 6, 7, 1.5, 8, 9, 12};

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

  public void testScore() throws IOException {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    ScoreCombinationIterator instance = new ScoreCombinationIterator(new NodeParameters(),
            iterators);

    ScoringContext context = new ScoringContext();
    one.setContext(context);
    two.setContext(context);
    instance.setContext(context);
    int[] lengths = new int[12];
    Arrays.fill(lengths, 100);
    for (int i = 0; i < 12; i++) {
      assertFalse(instance.isDone());
      assertTrue(instance.hasMatch(docsTogether[i]));
      context.document = docsTogether[i];
      assertEquals(scoresTogether[i], instance.score(), 0.0000001);

      instance.movePast(docsTogether[i]);
    }

    assertTrue(instance.isDone());
  }

  public void testMovePast() throws Exception {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    ScoreCombinationIterator instance = new ScoreCombinationIterator(new NodeParameters(),
            iterators);

    instance.movePast(5);
    assertEquals(6, instance.currentCandidate());
  }

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
