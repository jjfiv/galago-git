// BSD License (http://lemurproject.org/galago-license)
/*
 * ScoringFunctionIteratorTest.java
 * JUnit based test
 *
 * Created on September 14, 2007, 9:04 AM
 */
package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import junit.framework.*;
import java.io.IOException;
import org.lemurproject.galago.core.index.FakeLengthIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.scoring.ScoringFunction;

/**
 *
 * @author trevor
 */
public class ScoringFunctionIteratorTest extends TestCase {

  public ScoringFunctionIteratorTest(String testName) {
    super(testName);
  }

  public static class FakeScorer implements ScoringFunction {

    @Override
    public double score(int count, int length) {
      return count + length;
    }

    public String getParameterString() {
      return "fake";
    }
  }

  public static class FakeScoreIterator extends ScoringFunctionIterator {

    public FakeScoreIterator(MovableCountIterator iter) throws IOException {
      super(new NodeParameters(), iter, new FakeScorer());
    }

    public double scoreCount(int count, int length) {
      return count + length;
    }
  }

  public void testScore() throws IOException {

    int[][] data = {{1, 3}, {5, 8, 9}};
    FakeExtentIterator extentIterator = new FakeExtentIterator(data);
    FakeScoreIterator scoreIterator = new FakeScoreIterator(extentIterator);

    int[] docs = {1, 2, 5};
    int[] lengths = {3, 5, 10};
    FakeLengthIterator lengthIterator = new FakeLengthIterator(docs, lengths);

    ScoringContext ctx = new ScoringContext();
    ctx.addLength("", lengthIterator);
    lengthIterator.setContext(ctx);
    extentIterator.setContext(ctx);
    scoreIterator.setContext(ctx);
    
    assertFalse(scoreIterator.isDone());

    ctx.document = scoreIterator.currentCandidate();
    ctx.moveLengths(ctx.document);
    assertEquals(scoreIterator.currentCandidate(), 1);
    assertEquals(4.0, scoreIterator.score());
    scoreIterator.movePast(1);

    ctx.document = scoreIterator.currentCandidate();
    ctx.moveLengths(ctx.document);

    assertFalse(scoreIterator.isDone());
    assertEquals(12.0, scoreIterator.score());
    scoreIterator.movePast(5);

    assertTrue(scoreIterator.isDone());
  }
}
