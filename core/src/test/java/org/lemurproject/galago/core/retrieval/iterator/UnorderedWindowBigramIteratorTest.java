package org.lemurproject.galago.core.retrieval.iterator;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.extents.FakeExtentIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author jfoley.
 */
public class UnorderedWindowBigramIteratorTest {
  @Test
  public void testPhrase() throws IOException {
    int[][] dataOne = {{1, 3}};
    int[][] dataTwo = {{1, 4}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};

    NodeParameters twoParam = new NodeParameters();
    twoParam.set("default", 2);
    UnorderedWindowBigramIterator instance = new UnorderedWindowBigramIterator(twoParam, iters);

    ScoringContext context = new ScoringContext();
    context.document = instance.currentCandidate();
    assertEquals(1, context.document);

    ExtentArray array = instance.extents(context);
    assertFalse(instance.isDone());

    assertEquals(1, array.size());
    assertEquals(1, array.getDocument());
    assertEquals(3, array.begin(0));
    assertEquals(5, array.end(0));

    instance.movePast(instance.currentCandidate());
    context.document = instance.currentCandidate();

    assertTrue(instance.isDone());
  }

  @Test
  public void testBothWays() throws IOException {
    int[][] dataOne = {{121, 3,       15}};
    int[][] dataTwo = {{121,    4, 14,   17}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};

    NodeParameters twoParam = new NodeParameters();
    twoParam.set("default", 2);
    UnorderedWindowBigramIterator instance = new UnorderedWindowBigramIterator(twoParam, iters);

    ScoringContext context = new ScoringContext();
    context.document = instance.currentCandidate();
    assertEquals(121, context.document);

    ExtentArray array = instance.extents(context);
    assertFalse(instance.isDone());

    assertEquals(2, array.size());
    assertEquals(121, array.getDocument());
    assertEquals(3, array.begin(0));
    assertEquals(5, array.end(0));
    assertEquals(14, array.begin(1));
    assertEquals(16, array.end(1));

    instance.movePast(instance.currentCandidate());
    assertTrue(instance.isDone());
  }
}
