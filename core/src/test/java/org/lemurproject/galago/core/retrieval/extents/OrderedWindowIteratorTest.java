// BSD License (http://lemurproject.org/galago-license)
/*
 * OrderedWindowIteratorTest.java
 * JUnit based test
 *
 * Created on September 13, 2007, 7:00 PM
 */
package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.retrieval.iterator.OrderedWindowIterator;
import org.lemurproject.galago.core.util.ExtentArray;
import java.io.IOException;
import junit.framework.*;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author trevor
 */
public class OrderedWindowIteratorTest extends TestCase {

  public OrderedWindowIteratorTest(String testName) {
    super(testName);
  }

  public void testPhrase() throws IOException {
    int[][] dataOne = {{1, 3}};
    int[][] dataTwo = {{1, 4}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};

    NodeParameters oneParam = new NodeParameters();
    oneParam.set("default", 1);
    OrderedWindowIterator instance = new OrderedWindowIterator(oneParam, iters);

    ScoringContext cs = new ScoringContext();
    one.setContext(cs);
    two.setContext(cs);
    instance.setContext(cs);

    cs.document = instance.currentCandidate();    
    ExtentArray array = instance.extents();

    assertEquals(array.size(), 1);
    assertEquals(array.getDocument(), 1);
    assertEquals(array.begin(0), 3);
    assertEquals(array.end(0), 5);
  }

  public void testWrongOrder() throws IOException {
    int[][] dataOne = {{1, 3}};
    int[][] dataTwo = {{1, 4}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {two, one};

    NodeParameters oneParam = new NodeParameters();
    oneParam.set("default", 1);
    OrderedWindowIterator instance = new OrderedWindowIterator(oneParam, iters);

    ScoringContext cs = new ScoringContext();
    one.setContext(cs);
    two.setContext(cs);
    instance.setContext(cs);

    cs.document = instance.currentCandidate();    
    ExtentArray array = instance.extents();
    assertEquals(0, array.size());
  }
}
