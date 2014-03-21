// BSD License (http://lemurproject.org/galago-license)
/*
 * SynonymIteratorTest.java
 * JUnit based test
 *
 * Created on September 14, 2007, 8:58 AM
 */
package org.lemurproject.galago.core.retrieval.extents;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.iterator.SynonymIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor, sjh
 */
public class SynonymIteratorTest {
  @Test
  public void testNoData() throws IOException {
    int[][] dataOne = {};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator[] iters = {one};

    SynonymIterator instance = new SynonymIterator(new NodeParameters(), iters);

    ScoringContext context = new ScoringContext();
    context.document = instance.currentCandidate();
    
    assertTrue(instance.isDone());
  }

  @Test
  public void testTwoDocuments() throws IOException {
    int[][] dataOne = {{1, 3}};
    int[][] dataTwo = {{2, 4}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};
    
    SynonymIterator instance = new SynonymIterator(new NodeParameters(), iters);

    ScoringContext context = new ScoringContext();
    context.document = instance.currentCandidate();

    ExtentArray array = instance.extents(context);

    assertFalse(instance.isDone());
    assertEquals(1, array.size());
    assertEquals(1, array.getDocument());
    assertEquals(3, array.begin(0));
    assertEquals(4, array.end(0));

    instance.movePast(instance.currentCandidate());
    context.document = instance.currentCandidate();

    array = instance.extents(context);
    assertFalse(instance.isDone());
    assertEquals(1, array.size());
    assertEquals(2, array.getDocument());
    assertEquals(4, array.begin(0));
    assertEquals(5, array.end(0));

    instance.movePast(instance.currentCandidate());
    context.document = instance.currentCandidate();

    array = instance.extents(context);
    assertTrue(instance.isDone());
  }

  @Test
  public void testSameDocument() throws IOException {
    int[][] dataOne = {{1, 3}};
    int[][] dataTwo = {{1, 4}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};

    SynonymIterator instance = new SynonymIterator(new NodeParameters(), iters);

    ScoringContext context = new ScoringContext();

    context.document = instance.currentCandidate();
    
    ExtentArray array = instance.extents(context);

    assertFalse(instance.isDone());
    assertEquals(array.size(), 2);
    assertEquals(1, array.getDocument());
    assertEquals(array.begin(0), 3);
    assertEquals(array.end(0), 4);
    
    assertEquals(array.begin(1), 4);
    assertEquals(array.end(1), 5);

    instance.movePast(instance.currentCandidate());
    context.document = instance.currentCandidate();

    assertTrue(instance.isDone());
  }
}
