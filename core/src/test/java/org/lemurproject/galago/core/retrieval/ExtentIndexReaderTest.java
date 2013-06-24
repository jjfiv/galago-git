// BSD License (http://lemurproject.org/galago-license)
/*
 * ExtentIndexReaderTest.java
 * JUnit based test
 *
 * Created on October 5, 2007, 4:36 PM
 */
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.index.disk.DiskBTreeReader;
import org.lemurproject.galago.core.index.disk.WindowIndexWriter;
import org.lemurproject.galago.core.retrieval.iterator.ExtentArrayIterator;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.core.util.ExtentArray;
import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.WindowIndexReader;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class ExtentIndexReaderTest extends TestCase {

  File tempPath;

  public ExtentIndexReaderTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws Exception {
    // make a spot for the index
    tempPath = Utility.createTemporary();
    tempPath.delete();

    Parameters p = new Parameters();
    p.set("filename", tempPath.toString());

    WindowIndexWriter writer =
            new WindowIndexWriter(new org.lemurproject.galago.tupleflow.FakeParameters(p));

    writer.processExtentName(Utility.fromString("title"));
    writer.processNumber(1);
    writer.processBegin(2);
    writer.processTuple(3);
    writer.processBegin(10);
    writer.processTuple(11);

    writer.processNumber(9);
    writer.processBegin(5);
    writer.processTuple(10);

    writer.processExtentName(Utility.fromString("z"));
    writer.processNumber(15);
    writer.processBegin(9);
    writer.processTuple(11);

    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    if (tempPath != null) {
      tempPath.delete();
    }
  }

  public void testReadTitle() throws Exception {
    WindowIndexReader reader = new WindowIndexReader(new DiskBTreeReader(tempPath.toString()));
    ExtentIterator extents = reader.getTermExtents("title");
    extents.setContext(new ScoringContext());
    ScoringContext sc = extents.getContext();
    
    assertFalse(extents.isDone());

    assertEquals(1, extents.currentCandidate());
    sc.document = extents.currentCandidate();
    ExtentArray e = extents.extents();
    assertEquals(2, e.size());
    ExtentArrayIterator iter = new ExtentArrayIterator(e);
    assertFalse(iter.isDone());

    assertEquals(2, iter.currentBegin());
    assertEquals(3, iter.currentEnd());

    iter.next();
    assertFalse(iter.isDone());

    assertEquals(10, iter.currentBegin());
    assertEquals(11, iter.currentEnd());

    iter.next();
    assertTrue(iter.isDone());

    extents.movePast(extents.currentCandidate());
    assertFalse(extents.isDone());

    assertEquals(9, extents.currentCandidate());
    sc.document = extents.currentCandidate();
    e = extents.extents();
    iter = new ExtentArrayIterator(e);

    assertEquals(5, iter.currentBegin());
    assertEquals(10, iter.currentEnd());

    extents.movePast(extents.currentCandidate());
    assertTrue(extents.isDone());

    reader.close();
  }

  public void testReadZ() throws Exception {
    WindowIndexReader reader = new WindowIndexReader(new DiskBTreeReader(tempPath.toString()));
    ExtentIterator extents = reader.getTermExtents("z");
    extents.setContext(new ScoringContext());
    ScoringContext sc = extents.getContext();

    assertFalse(extents.isDone());

    assertEquals(15, extents.currentCandidate());
    sc.document = extents.currentCandidate();
    ExtentArray e = extents.extents();
    ExtentArrayIterator iter = new ExtentArrayIterator(e);

    assertEquals(9, iter.currentBegin());
    assertEquals(11, iter.currentEnd());

    extents.movePast(extents.currentCandidate());
    assertTrue(extents.isDone());

    reader.close();
  }

  public void testSimpleSkipTitle() throws Exception {
    WindowIndexReader reader = new WindowIndexReader(new DiskBTreeReader(tempPath.toString()));
    ExtentIterator extents = reader.getTermExtents("title");
    extents.setContext(new ScoringContext());
    ScoringContext sc = extents.getContext();

    assertFalse(extents.isDone());
    extents.syncTo(10);
    assertTrue(extents.isDone());

    reader.close();
  }

  public void testSkipList() throws Exception {
    Parameters p = new Parameters();
    p.set("filename", tempPath.toString());
    p.set("skipDistance", 10);

    WindowIndexWriter writer =
            new WindowIndexWriter(new org.lemurproject.galago.tupleflow.FakeParameters(p));

    writer.processExtentName(Utility.fromString("skippy"));
    for (int docid = 1; docid < 1000; docid += 3) {
      writer.processNumber(docid);
      for (int begin = 5; begin < (20 + (docid / 5)); begin += 4) {
        writer.processBegin(begin);
        writer.processTuple(begin + 2);
      }
    }
    writer.close();

    WindowIndexReader reader = new WindowIndexReader(new DiskBTreeReader(tempPath.toString()));
    ExtentIterator extents = reader.getTermExtents("skippy");
    extents.setContext(new ScoringContext());
    ScoringContext sc = extents.getContext();

    assertFalse(extents.isDone());
    extents.syncTo(453);
    assertFalse(extents.hasMatch(453));
    assertEquals(454, extents.currentCandidate());
    extents.movePast(extents.currentCandidate());
    assertEquals(457, extents.currentCandidate());
    sc.document = extents.currentCandidate();
    assertEquals(27, extents.count(sc));
    ExtentArray ea = extents.extents();
    ExtentArrayIterator eait = new ExtentArrayIterator(ea);
    int begin = 5;
    while (!eait.isDone()) {
      assertEquals(begin, eait.currentBegin());
      assertEquals(begin + 2, eait.currentEnd());
      begin += 4;
      eait.next();
    }
    extents.syncTo(1299);
    assertFalse(extents.hasMatch(1299));
    extents.movePast(2100);
    assertTrue(extents.isDone());
    reader.close();
  }
}
