// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.structured;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.BM25RFScoringIterator;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.FakeLengthIterator;
import org.lemurproject.galago.core.retrieval.extents.FakeExtentIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.scoring.ScoringFunction;

/**
 *
 * @author marc, sjh
 */
public class ScoringFunctionIteratorTest extends TestCase {

  int[][] extents = {
    {34, 55, 56, 57},
    {44, 67, 77},
    {110, 12, 23, 34}
  };

  private static class FakeScorer implements ScoringFunction {

    @Override
    public double score(int count, int length) {
      return (count + length);
    }
  }

  public void testGenericIterator() throws Exception {
    FakeExtentIterator extentIterator = new FakeExtentIterator(extents);
    int[] docs = {0, 34, 110};
    int[] lengths = {0, 99, 41};
    FakeLengthIterator lengthsIterator = new FakeLengthIterator(docs, lengths);

    ScoringFunctionIterator iterator = new ScoringFunctionIterator(new NodeParameters(), lengthsIterator, extentIterator);
    iterator.setScoringFunction(new FakeScorer());

    ScoringContext context = new ScoringContext();
    extentIterator.setContext(context);
    lengthsIterator.setContext(context);
    iterator.setContext(context);

    // check initial setup

    assertFalse(iterator.isDone());
    assertEquals(extents[0][0], iterator.currentCandidate());
    iterator.syncTo(extents[0][0]);
    assertEquals(extents[0][0], iterator.currentCandidate());

    // score with bad context.document
    context.document = 0;

    assertEquals(0.0, iterator.score());

    // score with good context.document
    context.document = 34;
    assertEquals(102.0, iterator.score());

    // score next document
    iterator.movePast(44);
    context.document = iterator.currentCandidate();
    iterator.syncTo(context.document);
    assertTrue(iterator.hasMatch(110));
    context.document = iterator.currentCandidate();
    assertEquals(44.0, iterator.score());

    iterator.syncTo(120);
    context.document = iterator.currentCandidate();
    assertTrue(iterator.isDone());
  }

  public void testBM25RFIterator() throws Exception {
    FakeExtentIterator extentIterator = new FakeExtentIterator(extents);
    
    int[] docs = {0, 34, 110};
    int[] lengths = {0, 99, 41};
    FakeLengthIterator lengthsIterator = new FakeLengthIterator(docs, lengths);

    NodeParameters p = new NodeParameters();
    p.set("rt", 3);
    p.set("R", 10);
    p.set("ft", 40);
    p.set("documentCount", 1000);
    p.set("factor", 0.45);    
    BM25RFScoringIterator iterator = new BM25RFScoringIterator(p, lengthsIterator, extentIterator);

    assertFalse(iterator.isDone());
    assertEquals(extents[0][0], iterator.currentCandidate());
    iterator.syncTo(extents[0][0]);
    assertEquals(extents[0][0], iterator.currentCandidate());

    // score without explicit context
    ScoringContext context = new ScoringContext();
    
    iterator.setContext(context);
    extentIterator.setContext(context);
    lengthsIterator.setContext(context);
    
    context.document = iterator.currentCandidate();
    assertEquals(1.11315, iterator.score(), 0.0001);

    iterator.movePast(44);
    context.document = iterator.currentCandidate();
    assertTrue(iterator.hasMatch(110));
    assertEquals(1.11315, iterator.score(), 0.0001);
    
    iterator.syncTo(120);
    context.document = iterator.currentCandidate();
    assertTrue(iterator.isDone());
  }
}
