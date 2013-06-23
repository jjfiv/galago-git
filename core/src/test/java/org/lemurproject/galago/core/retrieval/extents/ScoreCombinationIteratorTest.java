// BSD License (http://lemurproject.org/galago-license)
/*
 * ScoreCombinationIteratorTest.java
 * JUnit based test
 *
 * Created on October 9, 2007, 2:43 PM
 */
package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.retrieval.iterator.ScoreCombinationIterator;
import java.io.IOException;
import java.util.Arrays;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.FakeLengthIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class ScoreCombinationIteratorTest extends TestCase {

  int[] docsA = new int[]{5, 10, 15, 20};
  double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
  int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  int[] docsTogether = new int[]{2, 4, 5, 6, 8, 10, 12, 14, 15, 16, 18, 20};
  double[] scoresTogether = new double[]{1, 2, 0.5, 3, 4, 6, 6, 7, 1.5, 8, 9, 12};
  // sjh: weight testing
  double[] weights = new double[]{0.4, 1.6}; 
  double[] weightedScoresTogether = new double[]{3.2, 6.4, 0.4, 9.6, 12.8, 16.8, 19.2, 22.4, 1.2, 25.6, 28.8, 33.6};
  double[] normalWeightedScoresTogether = new double[]{1.6, 3.2, 0.2, 4.8, 6.4, 8.4, 9.6, 11.2, 0.6, 12.8, 14.4, 16.8};

  public ScoreCombinationIteratorTest(String testName) {
    super(testName);
  }

  public void testScore() throws IOException {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    ScoreCombinationIterator instance = new ScoreCombinationIterator(new NodeParameters(), iterators);
    int[] lengths = new int[12];
    Arrays.fill(lengths, 100);
    ScoringContext ctx = new ScoringContext();
    FakeLengthIterator fli = new FakeLengthIterator(docsTogether, lengths);
    one.setContext(ctx);
    two.setContext(ctx);
    for (int i = 0; i < 12; i++) {
      ctx.document = docsTogether[i];
      assertEquals(scoresTogether[i], instance.score(), 0.00001);
      instance.movePast(docsTogether[i]);
    }
  }

  public void testWeightedScore() throws IOException {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    NodeParameters weightParameters = new NodeParameters();
    weightParameters.set("0", weights[0]);
    weightParameters.set("1", weights[1]);
    ScoreCombinationIterator instance = new ScoreCombinationIterator(weightParameters, iterators);
    int[] lengths = new int[12];
    Arrays.fill(lengths, 100);
    ScoringContext ctx = new ScoringContext();
    //FakeLengthIterator fli = new FakeLengthIterator(docsTogether, lengths);
    one.setContext(ctx);
    two.setContext(ctx);
    for (int i = 0; i < 12; i++) {
      ctx.document = docsTogether[i];
      assert (Math.abs(normalWeightedScoresTogether[i] - instance.score()) < 0.000001);
      instance.movePast(docsTogether[i]);
    }
  }

  public void testUnnormalizedWeightings() throws IOException {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    NodeParameters weightParameters = new NodeParameters();
    weightParameters.set("0", weights[0]);
    weightParameters.set("1", weights[1]);
    weightParameters.set("norm", false);
    ScoreCombinationIterator instance = new ScoreCombinationIterator(weightParameters, iterators);
    int[] lengths = new int[12];
    Arrays.fill(lengths, 100);
    ScoringContext ctx = new ScoringContext();
    //FakeLengthIterator fli = new FakeLengthIterator(docsTogether, lengths);
    one.setContext(ctx);
    two.setContext(ctx);
    for (int i = 0; i < 12; i++) {
      ctx.document = docsTogether[i];
      assert (Math.abs(weightedScoresTogether[i] - instance.score()) < 0.000001);
      instance.movePast(docsTogether[i]);
    }
  }
  
  public void testMovePast() throws Exception {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    ScoreCombinationIterator instance = new ScoreCombinationIterator(new NodeParameters(), iterators);
    instance.movePast(5);
  }

  public void testMoveTo() throws Exception {
    FakeScoreIterator one = new FakeScoreIterator(docsA, scoresA);
    FakeScoreIterator two = new FakeScoreIterator(docsB, scoresB);
    FakeScoreIterator[] iterators = {one, two};

    ScoreCombinationIterator instance = new ScoreCombinationIterator(new NodeParameters(), iterators);

    instance.syncTo(5);
  }
}
