// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.MoveIterators;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.iterator.ExtentValueIterator;

/**
 *
 * @author trevor
 */
public class MoveIteratorsTest extends TestCase {
    private ExtentValueIterator[] iterators;
    
    public MoveIteratorsTest(String testName) {
        super(testName);
    }            

    @Override
    protected void setUp() throws Exception {
        ExtentValueIterator one = new FakeExtentIterator(new int[][] { {2,1}, {3,1}, {5,1} });
        ExtentValueIterator two = new FakeExtentIterator(new int[][] { {3,1}, {6,1}, {7,1} });
        iterators = new ExtentValueIterator[] { one, two };
    }
    
    /**
     * Test of moveAllToSameDocument method, of class MoveIterators.
     */
    public void testMoveAllToSameDocument() throws Exception {
        assertEquals(3, MoveIterators.moveAllToSameDocument(iterators));
        iterators[0].next();
        assertEquals(Integer.MAX_VALUE, MoveIterators.moveAllToSameDocument(iterators));
    }

    /**
     * Test of allSameDocument method, of class MoveIterators.
     */
    public void testAllSameDocument() throws IOException {
        assertFalse(MoveIterators.allSameDocument(iterators));
        iterators[0].next();
        assertTrue(MoveIterators.allSameDocument(iterators));
    }

    /**
     * Test of findMaximumDocument method, of class MoveIterators.
     */
    public void testFindMaximumDocument() throws IOException {
        assertEquals(3, MoveIterators.findMaximumDocument(iterators));
        iterators[0].next();
        assertEquals(3, MoveIterators.findMaximumDocument(iterators));
        iterators[1].next();
        assertEquals(6, MoveIterators.findMaximumDocument(iterators));
        iterators[1].next();
        assertEquals(7, MoveIterators.findMaximumDocument(iterators));
        iterators[1].next();
        assertEquals(Integer.MAX_VALUE, MoveIterators.findMaximumDocument(iterators));
    }
}
