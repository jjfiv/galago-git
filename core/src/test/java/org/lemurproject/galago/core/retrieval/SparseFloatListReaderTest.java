// BSD License (http://lemurproject.org/galago-license)
/*
 * SparseFloatListReaderTest.java
 * JUnit based test
 *
 * Created on October 9, 2007, 1:00 PM
 */
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.index.disk.SparseFloatListReader;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.core.index.disk.SparseFloatListWriter;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.FakeLengthIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class SparseFloatListReaderTest extends TestCase {

  private File tempPath;
  int[] aDocs = new int[]{5, 6};
  float[] aScores = new float[]{0.5f, 0.7f};
  int[] bDocs = new int[]{9, 11, 13};
  float[] bScores = new float[]{0.1f, 0.2f, 0.3f};

  public SparseFloatListReaderTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws IOException {
    // make a spot for the index
    tempPath = Utility.createTemporary();

    Parameters p = new Parameters();
    p.set("filename", tempPath.toString());

    TupleFlowParameters parameters = new FakeParameters(p);
    SparseFloatListWriter writer = new SparseFloatListWriter(parameters);

    writer.processWord(Utility.fromString("a"));

    for (int i = 0; i < aDocs.length; i++) {
      writer.processNumber(aDocs[i]);
      writer.processTuple(aScores[i]);
    }

    writer.processWord(Utility.fromString("b"));

    for (int i = 0; i < bDocs.length; i++) {
      writer.processNumber(bDocs[i]);
      writer.processTuple(bScores[i]);
    }

    writer.close();
  }

  @Override
  public void tearDown() throws IOException {
    tempPath.delete();
  }

  public void testA() throws Exception {
    SparseFloatListReader instance = new SparseFloatListReader(tempPath.toString());
    SparseFloatListReader.ListIterator iter = (SparseFloatListReader.ListIterator) instance.getIterator(new Node("scores", "a"));
    assertFalse(iter.isDone());
    int i;
    ScoringContext context = new ScoringContext();
    int[] lengths = new int[aDocs.length];
    Arrays.fill(lengths, 100);
    FakeLengthIterator fli = new FakeLengthIterator(aDocs, lengths);
    context.addLength("", fli);
    iter.setContext(context);
    for (i = 0; !iter.isDone(); i++) {
      assertEquals(aDocs[i], iter.currentCandidate());
      context.document = aDocs[i];
      assertEquals(aScores[i], iter.score(), 0.0001);
      assertTrue(iter.hasMatch(aDocs[i]));

      iter.movePast(aDocs[i]);
    }

    assertEquals(aDocs.length, i);
    assertTrue(iter.isDone());
  }

  public void testB() throws Exception {
    SparseFloatListReader instance = new SparseFloatListReader(tempPath.toString());
    SparseFloatListReader.ListIterator iter = (SparseFloatListReader.ListIterator) instance.getIterator(new Node("scores", "b"));
    int i;

    assertFalse(iter.isDone());
    ScoringContext ctx = new ScoringContext();
    int[] lengths = new int[bDocs.length];
    Arrays.fill(lengths, 100);
    FakeLengthIterator fli = new FakeLengthIterator(bDocs, lengths);
    ctx.addLength("", fli);
    iter.setContext(ctx);
    for (i = 0; !iter.isDone(); i++) {
      assertEquals(bDocs[i], iter.currentCandidate());
      ctx.document = bDocs[i];
      assertEquals(bScores[i], iter.score(), 0.0001);
      assertTrue(iter.hasMatch(bDocs[i]));

      iter.movePast(bDocs[i]);
    }

    assertEquals(bDocs.length, i);
    assertTrue(iter.isDone());
  }

  public void testIterator() throws Exception {
    SparseFloatListReader instance = new SparseFloatListReader(tempPath.toString());
    SparseFloatListReader.KeyValueIterator iter = instance.getIterator();
    String term = iter.getKeyString();

    assertEquals(term, "a");
    assertFalse(iter.isDone());

    MovableScoreIterator lIter = (MovableScoreIterator) iter.getValueIterator();
    ScoringContext context = new ScoringContext();
    int[] lengths = new int[aDocs.length];
    Arrays.fill(lengths, 100);
    FakeLengthIterator fli = new FakeLengthIterator(aDocs, lengths);
    fli.setContext(context);
    context.addLength("", fli);
    lIter.setContext(context);
    for (int i = 0; !lIter.isDone(); i++) {
      assertEquals(lIter.currentCandidate(), aDocs[i]);
      context.document = aDocs[i];
      assertEquals(lIter.score(), aScores[i], 0.0001);
      assertTrue(lIter.hasMatch(aDocs[i]));

      lIter.movePast(aDocs[i]);
    }

    assertTrue(iter.nextKey());
    term = iter.getKeyString();
    assertEquals(term, "b");
    assertFalse(iter.isDone());
    lIter = (MovableScoreIterator) iter.getValueIterator();

    context = new ScoringContext();
    lengths = new int[bDocs.length];
    Arrays.fill(lengths, 100);
    fli = new FakeLengthIterator(bDocs, lengths);
    context.addLength("", fli);
    fli.setContext(context);
    lIter.setContext(context);
    for (int i = 0; !lIter.isDone(); i++) {
      assertEquals(lIter.currentCandidate(), bDocs[i]);
      context.document = lIter.currentCandidate();
      context.moveLengths(context.document);
      assertEquals(lIter.score(), bScores[i], 0.0001);
      assertTrue(lIter.hasMatch(bDocs[i]));

      lIter.movePast(bDocs[i]);
    }
    assertTrue(lIter.isDone());
    assertFalse(iter.nextKey());
  }
}
