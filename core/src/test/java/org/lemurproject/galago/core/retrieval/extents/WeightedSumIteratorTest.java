// BSD License (http://lemurproject.org/galago-license)
/*
 * ScoreCombinationIteratorTest.java
 * JUnit based test
 *
 * Created on October 9, 2007, 2:43 PM
 */
package org.lemurproject.galago.core.retrieval.extents;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.iterator.WeightedSumIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class WeightedSumIteratorTest {

  private static final int[] docsA = new int[]{5, 10, 15, 20};
  private static final double[] scoresA = new double[]{-1.0, -2.0, -3.0, -4.0};
  private static final int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  private static final double[] scoresB = new double[]{-2, -4, -6, -8, -10, -12, -14, -16, -18, -20};
  private static final int[] docsTogether = new int[]{2, 4, 5, 6, 8, 10, 12, 14, 15, 16, 18, 20};
  private static final double[] scoresTogether = new double[]{-1.99999, -3.99999, -0.99999, -5.99999, -7.99999, -1.99966, -11.99999, -13.99998, -2.99999, -15.99987, -17.99908, -3.99999};
  // sjh: weight testing
  private static final double[] weights = new double[]{0.4, 1.6};
  private static final double[] normalWeightedScoresTogether = new double[]{-2.22314, -4.22314, -2.60943, -6.22314, -8.22314, -3.60809, -12.22314, -14.22313, -4.60943, -16.22311, -18.22291, -5.60943};
  private static final double[] unnormalweightedScoresTogether = new double[]{-1.52999, -3.52999, -1.91629, -5.52999, -7.52999, -2.91494, -11.52999, -13.52999, -3.91629, -15.52996, -17.52976, -4.91629};


  @Test
  public void testScore() throws IOException {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA, -25);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB, -25);
    FakeScoreIterator[] iterators = {one, two};

    WeightedSumIterator instance = new WeightedSumIterator(new NodeParameters(), iterators);
    int[] lengths = new int[12];
    Arrays.fill(lengths, 100);
    ScoringContext ctx = new ScoringContext();
    for (int i = 0; i < 12; i++) {
      ctx.document = docsTogether[i];
      assertEquals(scoresTogether[i], instance.score(ctx), 0.00001);
      instance.movePast(docsTogether[i]);
    }
  }

  @Test
  public void testNormalWeightedScore() throws IOException {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA, -25);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB, -25);
    FakeScoreIterator[] iterators = {one, two};

    NodeParameters weightParameters = new NodeParameters();
    weightParameters.set("0", weights[0]);
    weightParameters.set("1", weights[1]);
    weightParameters.set("norm", true);
    WeightedSumIterator instance = new WeightedSumIterator(weightParameters, iterators);
    int[] lengths = new int[12];
    Arrays.fill(lengths, 100);
    ScoringContext ctx = new ScoringContext();
    for (int i = 0; i < 12; i++) {
      ctx.document = docsTogether[i];
      assertEquals(normalWeightedScoresTogether[i], instance.score(ctx), 0.0001);
      instance.movePast(docsTogether[i]);
    }
  }

  @Test
  public void testUnnormalizedWeightings() throws IOException {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA, -25);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB, -25);
    FakeScoreIterator[] iterators = {one, two};

    NodeParameters weightParameters = new NodeParameters();
    weightParameters.set("0", weights[0]);
    weightParameters.set("1", weights[1]);
    weightParameters.set("norm", false);
    WeightedSumIterator instance = new WeightedSumIterator(weightParameters, iterators);
    int[] lengths = new int[12];
    Arrays.fill(lengths, 100);
    ScoringContext ctx = new ScoringContext();
    for (int i = 0; i < 12; i++) {
      ctx.document = docsTogether[i];
      assertEquals(unnormalweightedScoresTogether[i], instance.score(ctx), 0.0001);
      instance.movePast(docsTogether[i]);
    }
  }

  @Test
  public void testMovePast() throws Exception {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA, -25);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB, -25);
    FakeScoreIterator[] iterators = {one, two};

    WeightedSumIterator instance = new WeightedSumIterator(new NodeParameters(), iterators);
    instance.movePast(5);
  }

  @Test
  public void testMoveTo() throws Exception {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA, -25);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB, -25);
    FakeScoreIterator[] iterators = {one, two};

    WeightedSumIterator instance = new WeightedSumIterator(new NodeParameters(), iterators);

    instance.syncTo(5);
  }
}
