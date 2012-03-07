// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ScaleIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author marc
 */
public class ScaleIteratorTest extends TestCase {

    int[] docsA = new int[]{5, 10, 15, 20};
    double[] scoresA = new double[]{1.0, 2.0, 3.0, 4.0};
    int[] docsB = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
    double[] scoresB = new double[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20};

    public ScaleIteratorTest(String testName) {
        super(testName);
    }

    public void testA() throws Exception {
        FakeScoreIterator inner = new FakeScoreIterator(docsA, scoresA);
        ScaleIterator iterator = new ScaleIterator(new NodeParameters(), inner);
        assertFalse(iterator.isDone());
        assertTrue(iterator.atCandidate(docsA[0]));
        for (int i = 0; i < docsA.length; i++) {
            assertEquals(docsA[i], iterator.currentCandidate());
            assertEquals(scoresA[i], iterator.score(new ScoringContext(docsA[i], 100)));
            iterator.movePast(docsA[i]);
        }
        assertTrue(iterator.isDone());
        iterator.reset();
        assertTrue(iterator.atCandidate(docsA[0]));
    }

    public void testB() throws Exception {
       FakeScoreIterator inner = new FakeScoreIterator(docsB, scoresB);
       NodeParameters weightedParameters = new NodeParameters();
       weightedParameters.set("default", 0.5);
       ScaleIterator iterator = new ScaleIterator(weightedParameters, inner);
        assertFalse(iterator.isDone());
        assertTrue(iterator.atCandidate(docsB[0]));
        for (int i = 0; i < docsB.length; i++) {
            iterator.moveTo(docsB[i]);
            assertEquals(docsB[i], iterator.currentCandidate());
            assertEquals(scoresB[i]*0.5, iterator.score(new ScoringContext(docsB[i], 100)));
        }
        iterator.reset();
        assertTrue(iterator.atCandidate(docsB[0]));
    }
}
