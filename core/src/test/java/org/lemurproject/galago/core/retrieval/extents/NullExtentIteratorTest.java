// BSD License (http://lemurproject.org/galago-license)
/*
 * NullExtentIteratorTest.java
 * JUnit based test
 *
 * Created on September 13, 2007, 6:56 PM
 */

package org.lemurproject.galago.core.retrieval.extents;

import org.lemurproject.galago.core.retrieval.iterator.NullExtentIterator;
import junit.framework.*;

/**
 *
 * @author trevor
 */
public class NullExtentIteratorTest extends TestCase {
    public NullExtentIteratorTest(String testName) {
        super(testName);
    }

    public void testIsDone() {
        NullExtentIterator instance = new NullExtentIterator();
        assertEquals( true, instance.isDone() );
    }
}
