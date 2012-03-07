// BSD License (http://lemurproject.org/galago-license)
/*
 * ScoringFunctionIteratorTest.java
 * JUnit based test
 *
 * Created on September 14, 2007, 9:04 AM
 */

package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import junit.framework.*;
import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.scoring.ScoringFunction;

/**
 *
 * @author trevor
 */
public class ScoringFunctionIteratorTest extends TestCase {
    public ScoringFunctionIteratorTest(String testName) {
        super(testName);
    }

    public static class FakeScorer implements ScoringFunction {
        public double score(int count, int length) {
            return count + length;
        }

        public String getParameterString() {
          return "fake";
        }
    }

    public static class FakeScoreIterator extends ScoringFunctionIterator {
        public FakeScoreIterator( MovableCountIterator iter ) throws IOException {
            super(iter, new FakeScorer());
        }
        
        public double scoreCount(int count, int length) {
            return count + length;
        }
    }
    
    public void testScore() throws IOException {
        int document = 0;
        int length = 0;
        
        int[][] data = { { 1, 3 }, { 5, 8, 9 } };
        FakeExtentIterator iterator = new FakeExtentIterator( data );
        FakeScoreIterator instance = new FakeScoreIterator( iterator );
        
        assertFalse( instance.isDone() );
        
        assertEquals( instance.currentCandidate(), 1 );
        assertEquals( 4.0, instance.score(new ScoringContext(1, 3)));
        instance.movePast( 1 );

        assertFalse( instance.isDone() );
        assertEquals( instance.currentCandidate(), 5 );
        assertEquals( 5.0, instance.score(new ScoringContext(2, 5)));
        
        assertFalse( instance.isDone() );
        assertEquals( 12.0, instance.score(new ScoringContext(5, 10)) );
        instance.movePast( 5 );

        assertTrue( instance.isDone() );
    }
}
