 // BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ThresholdIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author marc
 */
public class ThresholdIteratorTest extends TestCase {

  int[] docsA = new int[]{5, 10, 15, 20};
  double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
  int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
  double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};

  public ThresholdIteratorTest(String testName) {
    super(testName);
  }

  public void testA() throws Exception {
    FakeScoreIterator inner = new FakeScoreIterator(docsA, scoresA);
    ScoringContext dc = new ScoringContext();
    inner.setContext(dc);
    
    NodeParameters dummyParameters = new NodeParameters();
    dummyParameters.set("raw", 2.5);
    ThresholdIterator iterator = new ThresholdIterator(new Parameters(), dummyParameters, inner);

    assertFalse(iterator.isDone());
    dc.document = iterator.currentCandidate();
    assertFalse(iterator.atCandidate(docsA[0]));
    iterator.movePast(docsA[0]);

    assertFalse(iterator.isDone());
    dc.document = iterator.currentCandidate();
    assertFalse(iterator.atCandidate(docsA[1]));
    iterator.movePast(docsA[1]);

    assertFalse(iterator.isDone());
    dc.document = iterator.currentCandidate();
    assertTrue(iterator.atCandidate(docsA[2]));
    iterator.movePast(docsA[2]);

    assertFalse(iterator.isDone());
    dc.document = iterator.currentCandidate();
    assertTrue(iterator.atCandidate(docsA[3]));
    iterator.movePast(docsA[3]);

    assertTrue(iterator.isDone());
    iterator.reset();

    dc.document = iterator.currentCandidate();
    assertFalse(iterator.atCandidate(docsA[0]));
    iterator.movePast(docsA[2]);
    
    assertFalse(iterator.isDone());
    dc.document = iterator.currentCandidate();
    assertTrue(iterator.atCandidate(docsA[3]));
  }
}
