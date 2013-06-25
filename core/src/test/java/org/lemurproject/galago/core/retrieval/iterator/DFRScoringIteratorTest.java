/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import junit.framework.TestCase;
import org.lemurproject.galago.core.index.FakeLengthIterator;
import org.lemurproject.galago.core.retrieval.extents.FakeExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.BiL2ScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.InL2ScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.PL2ScoringIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author sjh
 */
public class DFRScoringIteratorTest extends TestCase {

  public DFRScoringIteratorTest(String testName) {
    super(testName);
  }

  public void testPL2Scorer() throws Exception {
    // data : {{doc, begin}, ...}
    int[][] data = {{1, 3}, {2, 5}, {5, 11}};
    FakeExtentIterator extItr = new FakeExtentIterator(data);

    int[] ids = new int[]{0, 1, 2, 3, 4, 5};
    int[] lengths = new int[]{5, 10, 11, 12, 13, 14};
    FakeLengthIterator lenItr = new FakeLengthIterator(ids, lengths);

    NodeParameters np = new NodeParameters();
    np.set("collectionLength", 100);
    np.set("documentCount", 60);
    np.set("maximumCount", 11);
    np.set("nodeFrequency", 19);
    np.set("c", 2.0);

    ScoringContext context = new ScoringContext();
    PL2ScoringIterator scorer = new PL2ScoringIterator(np, lenItr, extItr);
    scorer.setContext(context);

    context.document = scorer.currentCandidate();

    double[] expected = new double[]{
      0.0, // doc 0
      0.502788, // doc 1
      0.463434, // doc 2
      0.0, // doc 3
      0.0, // doc 4
      0.364418 // doc 5
    };

    for (int i = 0; i < expected.length; i++) {
      context.document = i;
      scorer.syncTo(context.document);
      assertEquals(scorer.score(context), expected[i], 0.00001);
      scorer.movePast(context.document);
    }
  }

  public void testInL2Scorer() throws Exception {
    // data : {{doc, begin}, ...}
    int[][] data = {{1, 3}, {2, 5}, {5, 11}};
    FakeExtentIterator extItr = new FakeExtentIterator(data);

    int[] ids = new int[]{0, 1, 2, 3, 4, 5};
    int[] lengths = new int[]{5, 10, 11, 12, 13, 14};
    FakeLengthIterator lenItr = new FakeLengthIterator(ids, lengths);

    NodeParameters np = new NodeParameters();
    np.set("collectionLength", 100);
    np.set("documentCount", 60);
    np.set("maximumCount", 11);
    np.set("nodeFrequency", 19);
    np.set("c", 2.0);

    ScoringContext context = new ScoringContext();
    InL2ScoringIterator scorer = new InL2ScoringIterator(np, lenItr, extItr);
    scorer.setContext(context);

    context.document = scorer.currentCandidate();

    double[] expected = new double[]{
      0.0,
      2.03281,
      1.91526,
      0.0,
      0.0,
      1.63250
    };

    for (int i = 0; i < expected.length; i++) {
      context.document = i;
      scorer.syncTo(context.document);
      assertEquals(scorer.score(context), expected[i], 0.00001);
      scorer.movePast(context.document);
    }
  }

  public void testBiL2Scorer() throws Exception {
    // data : {{doc, begin}, ...}
    int[][] data = {{1, 3}, {2, 5}, {5, 11}};
    FakeExtentIterator extItr = new FakeExtentIterator(data);

    int[] ids = new int[]{0, 1, 2, 3, 4, 5};
    int[] lengths = new int[]{5, 10, 11, 12, 13, 14};
    FakeLengthIterator lenItr = new FakeLengthIterator(ids, lengths);

    NodeParameters np = new NodeParameters();
    np.set("collectionLength", 100);
    np.set("documentCount", 60);
    np.set("maximumCount", 11);
    np.set("nodeFrequency", 19);
    np.set("c", 2.0);

    ScoringContext context = new ScoringContext();
    BiL2ScoringIterator scorer = new BiL2ScoringIterator(np, lenItr, extItr);
    scorer.setContext(context);

    context.document = scorer.currentCandidate();

    double[] expected = new double[]{
      0.0,
      2.788971,
      3.012237,
      0.0,
      0.0,
      3.599482
    };

    for (int i = 0; i < expected.length; i++) {
      context.document = i;
      scorer.syncTo(context.document);
      assertEquals(scorer.score(context), expected[i], 0.00001);
      scorer.movePast(context.document);
    }
  }
}
