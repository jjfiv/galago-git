// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.structured;

import org.junit.Test;
import org.lemurproject.galago.core.index.FakeLengthIterator;
import org.lemurproject.galago.core.retrieval.extents.FakeExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.scoring.BM25RFScoringIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author marc, sjh
 */
public class ScoringFunctionIteratorTest {

  private static final int[][] extents = {
    {34, 55, 56, 57},
    {44, 67, 77},
    {110, 12, 23, 34}
  };

  private static class FakeScorer extends ScoringFunctionIterator {

    public FakeScorer(NodeParameters p, LengthsIterator l, CountIterator c) throws IOException {
      super(p, l, c);
    }

    public double score(int count, int length) {
      return (count + length);
    }

    @Override
    public double score(ScoringContext c) {
      return ((CountIterator) this.iterator).count(c) + this.lengthsIterator.length(c);
    }
  }

  @Test
  public void testGenericIterator() throws Exception {
    FakeExtentIterator extentIterator = new FakeExtentIterator(extents);
    int[] docs = {0, 34, 110};
    int[] lengths = {0, 99, 41};
    FakeLengthIterator lengthsIterator = new FakeLengthIterator(docs, lengths);

    ScoringFunctionIterator iterator = new FakeScorer(new NodeParameters(), lengthsIterator, extentIterator);

    ScoringContext context = new ScoringContext();

    // check initial setup

    assertFalse(iterator.isDone());
    assertEquals(extents[0][0], iterator.currentCandidate());
    iterator.syncTo(extents[0][0]);
    assertEquals(extents[0][0], iterator.currentCandidate());

    // score with bad context.document
    context.document = 0;

    assertEquals(0.0, iterator.score(context), 0.001);

    // score with good context.document
    context.document = 34;
    assertEquals(102.0, iterator.score(context), 0.001);

    // score next document
    iterator.movePast(44);
    context.document = iterator.currentCandidate();
    iterator.syncTo(context.document);
    assertTrue(iterator.hasMatch(new ScoringContext(110)));
    context.document = iterator.currentCandidate();
    assertEquals(44.0, iterator.score(context), 0.001);

    iterator.syncTo(120);
    context.document = iterator.currentCandidate();
    assertTrue(iterator.isDone());
  }

  @Test
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

    context.document = iterator.currentCandidate();
    assertEquals(1.11315, iterator.score(context), 0.0001);

    iterator.movePast(44);
    context.document = iterator.currentCandidate();
    assertTrue(iterator.hasMatch(new ScoringContext(110)));
    assertEquals(1.11315, iterator.score(context), 0.0001);

    iterator.syncTo(120);
    context.document = iterator.currentCandidate();
    assertTrue(iterator.isDone());
  }
}
