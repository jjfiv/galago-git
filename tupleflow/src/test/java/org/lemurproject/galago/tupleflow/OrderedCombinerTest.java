package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.OrderedCombiner;
import junit.framework.*;

/**
 *
 * @author trevor
 */
public class OrderedCombinerTest extends TestCase {
    public OrderedCombinerTest(String testName) {
        super(testName);
    }

    public void testGetOutputClass() {
        OrderedCombiner instance =
                new OrderedCombiner(new TypeReader[0], new FakeType().getOrder("+value"));

        Class expResult = FakeType.class;
        Class result = instance.getOutputClass();
        assertEquals(expResult, result);
    }

    public void testRun() throws Exception {
        OrderedCombiner instance =
                new OrderedCombiner(new TypeReader[0], new FakeType().getOrder("+value"));
        instance.run();
    }
}
