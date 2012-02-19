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
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ScoreValueIterator;
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
        SparseFloatListReader.ListIterator iter = instance.getScores("a");
        assertFalse(iter.isDone());
        int i;
        ScoringContext context = new ScoringContext();
        iter.setContext(context);
        for (i = 0; !iter.isDone(); i++) {
            assertEquals(aDocs[i], iter.currentCandidate());
            context.document = aDocs[i];
            context.length = 100;
            assertEquals(aScores[i], iter.score(), 0.0001);
            assertTrue(iter.hasMatch(aDocs[i]));

            iter.movePast(aDocs[i]);
        }

        assertEquals(aDocs.length, i);
        assertTrue(iter.isDone());
    }

    public void testB() throws Exception {
        SparseFloatListReader instance = new SparseFloatListReader(tempPath.toString());
        SparseFloatListReader.ListIterator iter = instance.getScores("b");
        int i;

        assertFalse(iter.isDone());

        for (i = 0; !iter.isDone(); i++) {
            assertEquals(bDocs[i], iter.currentCandidate());
            assertEquals(bScores[i], iter.score(new ScoringContext(bDocs[i], 100)), 0.0001);
            assertTrue(iter.hasMatch(bDocs[i]));

            iter.movePast(bDocs[i]);
        }

        assertEquals(bDocs.length, i);
        assertTrue(iter.isDone());
    }

    public void testIterator() throws Exception {
        SparseFloatListReader instance = new SparseFloatListReader(tempPath.toString());
        SparseFloatListReader.Iterator iter = instance.getIterator();
        String term = iter.getKey();

        assertEquals(term, "a");
        assertFalse(iter.isDone());

        ScoreValueIterator lIter = (ScoreValueIterator) iter.getValueIterator();
        ScoringContext context = new ScoringContext();
        lIter.setContext(context);
        for (int i = 0; !lIter.isDone(); i++) {
            assertEquals(lIter.currentCandidate(), aDocs[i]);
            context.document = aDocs[i];
            context.length = 100;
            assertEquals(lIter.score(), aScores[i], 0.0001);
            assertTrue(lIter.hasMatch(aDocs[i]));

            lIter.movePast(aDocs[i]);
        }

        assertTrue(iter.nextKey());
        term = iter.getKey();
        assertEquals(term, "b");
        assertFalse(iter.isDone());
        lIter = (ScoreValueIterator) iter.getValueIterator();
        for (int i = 0; !lIter.isDone(); i++) {
            assertEquals(lIter.currentCandidate(), bDocs[i]);
            assertEquals(lIter.score(new ScoringContext(bDocs[i], 100)), bScores[i], 0.0001);
            assertTrue(lIter.hasMatch(bDocs[i]));

            lIter.movePast(bDocs[i]);
        }
        assertFalse(lIter.next());
        assertFalse(iter.nextKey());
    }
}
