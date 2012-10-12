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
import org.lemurproject.galago.core.retrieval.iterator.MovableLengthsIterator;
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

    public FakeScoreIterator(MovableLengthsIterator ls, MovableCountIterator iter) throws IOException {
      super(new NodeParameters(), ls, iter);
      this.setScoringFunction(new FakeScorer());
    }

    public double scoreCount(int count, int length) {
      return count + length;
    }
  }

  public void testScore() throws IOException {

    int[][] data = {{1, 3}, {5, 8, 9}};
    int[] docs = {1, 2, 5};
    int[] lengths = {3, 5, 10};
    FakeExtentIterator iterator = new FakeExtentIterator(data);
    FakeLengthIterator lens = new FakeLengthIterator(docs, lengths);
    FakeScoreIterator instance = new FakeScoreIterator(lens, iterator);
    
    ScoringContext ctx = new ScoringContext();
    ctx.addLength("", lens);
    instance.setContext(ctx);
    lens.setContext(ctx);
    iterator.setContext(ctx);
    
    assertFalse(instance.isDone());

    ctx.document = instance.currentCandidate();
    ctx.moveLengths(ctx.document);
    assertEquals(instance.currentCandidate(), 1);
    assertEquals(4.0, instance.score());
    instance.movePast(1);

    ctx.document = instance.currentCandidate();
    ctx.moveLengths(ctx.document);

    assertFalse(instance.isDone());
    assertEquals(12.0, instance.score());
    instance.movePast(5);

    assertTrue(instance.isDone());
  }
}
