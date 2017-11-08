// BSD License (http://lemurproject.org/galago-license)
/*
 * OrderedWindowIteratorTest.java
 * JUnit based test
 *
 * Created on September 13, 2007, 7:00 PM
 */
package org.lemurproject.galago.core.retrieval.extents;

import org.junit.Test;
import org.lemurproject.galago.core.index.mem.MemoryIndex;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.OrderedWindowIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

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

  @Test
  public void testStats() throws Exception {
    try (MemoryIndex index = new MemoryIndex()) {
      Document doc1 = new Document();
      Document doc2 = new Document();
      doc1.name = "h2";
      doc1.text = "hello world is hello world";
      doc1.terms = Arrays.asList(doc1.text.split("\\s+"));
      doc1.tags = Collections.emptyList();
      doc2.name = "h1";
      doc2.text = "hello world is the best hello";
      doc2.terms = Arrays.asList(doc2.text.split("\\s+"));
      doc2.tags = Collections.emptyList();

      index.process(doc1);
      index.process(doc2);


      LocalRetrieval ret = new LocalRetrieval(index);
      Node phrase = StructuredQuery.parse("#od:1(hello world)");

      CountIterator iterator = (CountIterator) ret.createIterator(Parameters.create(), ret.transformQuery(phrase.clone(), Parameters.create()));

      iterator.syncTo(0);
      int count = iterator.count(new ScoringContext(0));
      assertEquals(2, count);
      iterator.syncTo(1);
      count = iterator.count(new ScoringContext(1));
      assertEquals(1, count);



      NodeStatistics stats = ret.getNodeStatistics(ret.transformQuery(phrase, Parameters.create()));
      assertEquals(2, stats.nodeDocumentCount);
      assertEquals(3, stats.nodeFrequency);
    }

  }
}
