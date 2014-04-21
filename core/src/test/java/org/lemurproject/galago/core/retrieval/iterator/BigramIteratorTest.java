package org.lemurproject.galago.core.retrieval.iterator;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.extents.FakeExtentIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author jfoley.
 */
public class BigramIteratorTest {
  @Test
  public void testPhrase() throws IOException {
    int[][] dataOne = {{1, 3}};
    int[][] dataTwo = {{1, 4}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};

    NodeParameters oneParam = new NodeParameters();
    oneParam.set("default", 1);
    BigramIterator instance = new BigramIterator(oneParam, iters);

    ScoringContext context = new ScoringContext();

    context.document = instance.currentCandidate();
    System.out.println("currentCandidate:"+context.document);
    ExtentArray array = instance.extents(context);

    assertEquals(array.size(), 1);
    assertEquals(array.getDocument(), 1);
    assertEquals(array.begin(0), 3);
    assertEquals(array.end(0), 5);
  }

  @Test
  public void testPhrase2() throws IOException {
    int[][] dataOne = {{13,    2, 3,    5, 6,                      30,     54}};
    int[][] dataTwo = {{13, 1,       4,       7, 8, 9, 10, 11, 17,     31,     55}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};

    NodeParameters oneParam = new NodeParameters();
    oneParam.set("default", 1);
    BigramIterator instance = new BigramIterator(oneParam, iters);

    ScoringContext context = new ScoringContext();

    context.document = instance.currentCandidate();
    assertEquals(13, context.document);
    ExtentArray array = instance.extents(context);

    assertEquals(4, array.size());
    assertEquals(13, array.getDocument());
    assertEquals(3, array.begin(0));
    assertEquals(5, array.end(0));
    assertEquals(6, array.begin(1));
    assertEquals(30, array.begin(2));
    assertEquals(54, array.begin(3));
  }
}
