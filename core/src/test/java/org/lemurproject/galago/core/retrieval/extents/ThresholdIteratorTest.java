 // BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

 import org.junit.Test;
 import org.lemurproject.galago.core.retrieval.iterator.ThresholdIterator;
 import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
 import org.lemurproject.galago.core.retrieval.query.NodeParameters;

 import static org.junit.Assert.assertFalse;
 import static org.junit.Assert.assertTrue;

 /**
 *
 * @author marc
 */
public class ThresholdIteratorTest {

  private static final int[] docsA = new int[]{5, 10, 15, 20};
  private static final double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};

  @Test
  public void testA() throws Exception {
    FakeScoreIterator inner = new FakeScoreIterator(docsA, scoresA);
    ScoringContext dc = new ScoringContext();
    
    NodeParameters dummyParameters = new NodeParameters();
    dummyParameters.set("raw", 2.5);
    ThresholdIterator iterator = new ThresholdIterator(dummyParameters, inner);

    assertFalse(iterator.isDone());
    dc.document = iterator.currentCandidate();
    assertTrue(iterator.hasMatch(docsA[0]));
    assertFalse(iterator.indicator(dc));
    iterator.movePast(docsA[0]);

    assertFalse(iterator.isDone());
    dc.document = iterator.currentCandidate();
    assertTrue(iterator.hasMatch(docsA[1]));
    assertFalse(iterator.indicator(dc));
    iterator.movePast(docsA[1]);

    assertFalse(iterator.isDone());
    dc.document = iterator.currentCandidate();
    assertTrue(iterator.hasMatch(docsA[2]));
    assertTrue(iterator.indicator(dc));
    iterator.movePast(docsA[2]);

    assertFalse(iterator.isDone());
    dc.document = iterator.currentCandidate();
    assertTrue(iterator.hasMatch(docsA[3]));
    assertTrue(iterator.indicator(dc));
    iterator.movePast(docsA[3]);

    assertTrue(iterator.isDone());
    iterator.reset();

    dc.document = iterator.currentCandidate();
    assertTrue(iterator.hasMatch(docsA[0]));
    assertFalse(iterator.indicator(dc));
    iterator.movePast(docsA[2]);
    
    assertFalse(iterator.isDone());
    dc.document = iterator.currentCandidate();
    assertTrue(iterator.hasMatch(docsA[3]));
    assertTrue(iterator.indicator(dc));
  }
}
