// BSD License (http://lemurproject.org/galago-license)
/*
 * OrderedWindowIteratorTest.java
 * JUnit based test
 *
 * Created on September 13, 2007, 7:00 PM
 */
package org.lemurproject.galago.core.retrieval.extents;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.iterator.OrderedWindowIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class OrderedWindowIteratorTest {
  @Test
  public void testPhrase() throws IOException {
    int[][] dataOne = {{1, 3}};
    int[][] dataTwo = {{1, 4}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};

    NodeParameters oneParam = new NodeParameters();
    oneParam.set("default", 1);
    OrderedWindowIterator instance = new OrderedWindowIterator(oneParam, iters);

    ScoringContext context = new ScoringContext();

    context.document = instance.currentCandidate();
    ExtentArray array = instance.extents(context);

    assertEquals(array.size(), 1);
    assertEquals(array.getDocument(), 1);
    assertEquals(array.begin(0), 3);
    assertEquals(array.end(0), 5);
  }

  @Test
  public void testWrongOrder() throws IOException {
    int[][] dataOne = {{1, 3}};
    int[][] dataTwo = {{1, 4}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    // note the order is backwards
    FakeExtentIterator[] iters = {two, one};

    NodeParameters oneParam = new NodeParameters();
    oneParam.set("default", 1);
    OrderedWindowIterator instance = new OrderedWindowIterator(oneParam, iters);

    ScoringContext context = new ScoringContext();

    context.document = instance.currentCandidate();    
    ExtentArray array = instance.extents(context);
    assertEquals(0, array.size());
  }
}
