// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.retrieval.iterator.UnorderedWindowIterator;
import java.io.IOException;
import junit.framework.*;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class UnorderedWindowIteratorTest extends TestCase {
    public UnorderedWindowIteratorTest(String testName) {
        super(testName);
    }

    public void testPhrase() throws IOException {
        int[][] dataOne = {{1, 3}};
        int[][] dataTwo = {{1, 4}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { one, two };
        
        NodeParameters twoParam = new NodeParameters();
        twoParam.set("default", 2);
        UnorderedWindowIterator instance = new UnorderedWindowIterator(new Parameters(), twoParam, iters);

        ExtentArray array = instance.extents();
        assertFalse(instance.isDone());

        assertEquals(1, array.size());
        assertEquals(1, array.getDocument());
        assertEquals(3, array.begin(0));
        assertEquals(5, array.end(0));

        instance.next();
        assertTrue(instance.isDone());
    }

    public void testUnordered() throws IOException {
        int[][] dataOne = {{1, 3}};
        int[][] dataTwo = {{1, 4}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { one, two };

        NodeParameters twoParam = new NodeParameters();
        twoParam.set("default", 2);
        UnorderedWindowIterator instance = new UnorderedWindowIterator(new Parameters(), twoParam, iters);
        ExtentArray array = instance.extents();
        assertFalse(instance.isDone());

        assertEquals(array.size(), 1);
        assertEquals(1, array.getDocument());
        assertEquals(array.begin(0), 3);
        assertEquals(array.end(0), 5);

        instance.next();
        assertTrue(instance.isDone());
    }

    public void testDifferentDocuments() throws IOException {
        int[][] dataOne = {{2, 3}};
        int[][] dataTwo = {{1, 4}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { one, two };

        NodeParameters twoParam = new NodeParameters();
        twoParam.set("default",2);

        Parameters params = new Parameters();
        params.set("shareNodes", true);
        
        UnorderedWindowIterator instance = new UnorderedWindowIterator(params, twoParam, iters);
        ExtentArray array = instance.extents();
        assertEquals(0, array.size());
        assertTrue( ! instance.isDone() );
        instance.movePast( instance.currentCandidate() );
        assertTrue( instance.isDone() );
    }

    public void testMultipleDocuments() throws IOException {
        int[][] dataOne = {{1, 3}, {2, 5}, {5, 11}};
        int[][] dataTwo = {{1, 4}, {3, 8}, {5, 9}};
        FakeExtentIterator one = new FakeExtentIterator(dataOne);
        FakeExtentIterator two = new FakeExtentIterator(dataTwo);
        FakeExtentIterator[] iters = { one, two };

        NodeParameters fiveParam = new NodeParameters();
        fiveParam.set("width", 5);

        UnorderedWindowIterator instance = new UnorderedWindowIterator(new Parameters(), fiveParam, iters);
        ExtentArray array = instance.extents();
        assertFalse(instance.isDone());

        assertEquals(array.size(), 1);
        assertEquals(1, array.getDocument());
        assertEquals(array.begin(0), 3);
        assertEquals(array.end(0), 5);

        // move to 2
        instance.next();
        assertFalse(instance.isDone());

        // move to 4
        instance.next();
        assertFalse(instance.isDone());

        // move to 5
        instance.next();
        assertFalse(instance.isDone());
        
        assertEquals(array.size(), 1);
        assertEquals(5, array.getDocument(), 5);
        assertEquals(array.begin(0), 9);
        assertEquals(array.end(0), 12);

        instance.next();
        assertTrue(instance.isDone());
    }
}
