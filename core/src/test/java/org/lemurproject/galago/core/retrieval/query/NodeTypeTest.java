// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.query;

import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Date;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class NodeTypeTest extends TestCase {
    
    public NodeTypeTest(String testName) {
        super(testName);
    }

    public void testGetIteratorClass() {
        NodeType n = new NodeType(ExtentIterator.class);
        assertEquals(ExtentIterator.class, n.getIteratorClass());
    }
    
    public void testIsStructuredIteratorOrArray() {
        NodeType n = new NodeType(ExtentIterator.class);
        assertTrue(n.isStructuredIteratorOrArray(ExtentIterator.class));
        assertTrue(n.isStructuredIteratorOrArray(StructuredIterator.class));
        assertFalse(n.isStructuredIteratorOrArray(Integer.class));
        assertFalse(n.isStructuredIteratorOrArray(Date.class));
        assertTrue(n.isStructuredIteratorOrArray(new ExtentIterator[0].getClass()));
    }

    public static class FakeIterator implements StructuredIterator {
        public FakeIterator(Parameters globalParams, NodeParameters parameters, ExtentIterator one, StructuredIterator[] two) {
        }

        public void reset() throws IOException {
        }

        public boolean isDone() {
          return true;
        }
    }
    
    public void testGetInputs() throws Exception {
        NodeType n = new NodeType(FakeIterator.class);
        Class[] input = n.getInputs();
        assertEquals(4, input.length);
        assertEquals(Parameters.class, input[0]);
        assertEquals(NodeParameters.class, input[1]);
        assertEquals(ExtentIterator.class, input[2]);
        assertEquals(new StructuredIterator[0].getClass(), input[3]);
    }
    
    public void testGetParameterTypes() throws Exception {
        NodeType n = new NodeType(FakeIterator.class);
        Class[] input = n.getParameterTypes(4);
        assertEquals(4, input.length);
        assertEquals(ExtentIterator.class, input[0]);
        assertEquals(StructuredIterator.class, input[1]);
        assertEquals(StructuredIterator.class, input[2]);
        assertEquals(StructuredIterator.class, input[3]);
    }
    
    public void testGetConstructor() throws Exception {
        NodeType n = new NodeType(FakeIterator.class);
        Constructor c = n.getConstructor();
        Constructor actual =
                FakeIterator.class.getConstructor(Parameters.class, NodeParameters.class, ExtentIterator.class,
                                                  new StructuredIterator[0].getClass());
        assertEquals(actual, c);
    }
}
