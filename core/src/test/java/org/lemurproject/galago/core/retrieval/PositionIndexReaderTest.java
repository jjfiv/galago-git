// BSD License (http://lemurproject.org/galago-license)
/*
 * PositionIndexReaderTest.java
 * JUnit based test
 *
 * Created on October 5, 2007, 4:38 PM
 */
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.core.index.disk.PositionIndexWriter;
import org.lemurproject.galago.core.retrieval.iterator.ExtentArrayIterator;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.core.util.ExtentArray;
import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.TermCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskExtentIterator;
import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskCountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 *
 * @author trevor
 */
public class PositionIndexReaderTest extends TestCase {

  File tempPath;
  File skipPath = null;
  static int[][] dataA = {
    {5, 7, 9},
    {19, 27, 300}
  };
  static int[][] dataB = {
    {149, 15500, 30319},
    {555555, 2}
  };

  public PositionIndexReaderTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws Exception {
    // make a spot for the index
    tempPath = Utility.createTemporary();
    tempPath.delete();

    skipPath = Utility.createTemporary();
    skipPath.delete();

    Parameters p = new Parameters();
    p.set("filename", tempPath.toString());
    p.set("estimateDocumentCount", true);

    PositionIndexWriter writer =
            new PositionIndexWriter(new org.lemurproject.galago.tupleflow.FakeParameters(p));

    writer.processWord(Utility.fromString("a"));

    for (int[] doc : dataA) {
      writer.processDocument(doc[0]);

      for (int i = 1; i < doc.length; i++) {
        writer.processPosition(doc[i]);
      }
    }

    writer.processWord(Utility.fromString("b"));

    for (int[] doc : dataB) {
      writer.processDocument(doc[0]);

      for (int i = 1; i < doc.length; i++) {
        writer.processPosition(doc[i]);
      }
    }

    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    tempPath.delete();
    if (skipPath != null) {
      skipPath.delete();
    }
  }

  public void internalTestIterator(
          ExtentIterator termExtents,
          int[][] data) throws IOException {
    assertNotNull(termExtents);
    assertFalse(termExtents.isDone());
    assertEquals(data.length, termExtents.totalEntries());
    int totalPositions = 0;

    ScoringContext sc = termExtents.getContext();

    for (int[] doc : data) {
      assertFalse(termExtents.isDone());
      sc.document = termExtents.currentCandidate();

      ExtentArray e = termExtents.extents();
      ExtentArrayIterator iter = new ExtentArrayIterator(e);
      totalPositions += (doc.length - 1); // first entry in doc array is docid
      for (int i = 1; i < doc.length; i++) {
        assertFalse(iter.isDone());
        assertEquals(doc[i], iter.currentBegin());
        assertEquals(doc[i] + 1, iter.currentEnd());
        iter.next();
      }
      assertTrue(iter.isDone());
      termExtents.movePast(termExtents.currentCandidate());
    }

    assertEquals(((NodeAggregateIterator) termExtents).getStatistics().nodeFrequency, totalPositions);
    assertTrue(termExtents.isDone());
  }

  public void testA() throws Exception {
    PositionIndexReader reader = new PositionIndexReader(tempPath.toString());
    ExtentIterator termExtents = reader.getTermExtents("a");
    termExtents.setContext(new ScoringContext());

    internalTestIterator(termExtents, dataA);
    NodeStatistics a_stats = ((NodeAggregateIterator) termExtents).getStatistics();
    assertEquals(2, a_stats.nodeDocumentCount);
    assertEquals(4, a_stats.nodeFrequency);
    reader.close();
  }

  public void testB() throws Exception {
    PositionIndexReader reader = new PositionIndexReader(tempPath.toString());
    ExtentIterator termExtents = reader.getTermExtents("b");
    termExtents.setContext(new ScoringContext());

    internalTestIterator(termExtents, dataB);
    NodeStatistics b_stats = ((NodeAggregateIterator) termExtents).getStatistics();
    assertEquals(2, b_stats.nodeDocumentCount);
    assertEquals(3, b_stats.nodeFrequency);
    reader.close();
  }

  public void testSkipLists() throws Exception {
    // internally fill the skip file
    Parameters p = new Parameters();
    p.set("filename", skipPath.toString());
    p.set("skipping", true);
    p.set("skipDistance", 20);
    p.set("skipResetDistance", 5);
    p.set("estimateDocumentCount", true);

    PositionIndexWriter writer =
            new PositionIndexWriter(new org.lemurproject.galago.tupleflow.FakeParameters(p));

    writer.processWord(Utility.fromString("a"));
    for (int docid = 1; docid < 5000; docid += 3) {
      writer.processDocument(docid);
      for (int pos = 1; pos < ((docid / 50) + 2); pos++) {
        writer.processPosition(pos);
      }
    }
    writer.close();

    // Now read it
    PositionIndexReader reader = new PositionIndexReader(skipPath.toString());
    DiskExtentIterator termExtents = reader.getTermExtents("a");
    termExtents.setContext(new ScoringContext());
    ScoringContext sc = termExtents.getContext();

    assertEquals("a", termExtents.getKeyString());

    // Read first identifier
    assertEquals(1, termExtents.currentCandidate());
    sc.document = termExtents.currentCandidate();
    assertEquals(1, termExtents.count());

    termExtents.syncTo(7);
    assertTrue(termExtents.hasMatch(7));

    // Now move to a doc, but not one we have
    termExtents.syncTo(90);
    assertFalse(termExtents.hasMatch(90));

    // Now move forward one
    termExtents.movePast(93);
    assertEquals(94, termExtents.currentCandidate());
    sc.document = termExtents.currentCandidate();
    assertEquals(2, termExtents.count());

    // One more time, then we read extents
    termExtents.movePast(2543);
    assertEquals(2545, termExtents.currentCandidate());
    sc.document = termExtents.currentCandidate();
    assertEquals(51, termExtents.count());
    ExtentArray ea = termExtents.extents();
    assertEquals(2545, ea.getDocument());
    assertEquals(51, ea.size());
    for (int i = 0; i < ea.size(); i++) {
      assertEquals(i + 1, ea.begin(i));
    }
    termExtents.syncTo(10005);
    assertFalse(termExtents.hasMatch(10005));
    assertTrue(termExtents.isDone());

    skipPath.delete();
    skipPath = null;
  }

  public void testCountIterator() throws Exception {
    PositionIndexReader reader = new PositionIndexReader(tempPath.toString());
    DiskCountIterator termCounts = reader.getTermCounts("b");

    termCounts.setContext(new ScoringContext());
    ScoringContext sc = termCounts.getContext();

    assertEquals(dataB[0][0], termCounts.currentCandidate());
    sc.document = termCounts.currentCandidate();
    assertEquals(dataB[0].length - 1, termCounts.count());
    termCounts.movePast(dataB[0][0]);

    assertEquals(dataB[1][0], termCounts.currentCandidate());
    sc.document = termCounts.currentCandidate();
    assertEquals(dataB[1].length - 1, termCounts.count());

    NodeStatistics b_stats = ((NodeAggregateIterator) termCounts).getStatistics();
    assertEquals(2, b_stats.nodeDocumentCount);
    assertEquals(3, b_stats.nodeFrequency);

    reader.close();
  }
}
