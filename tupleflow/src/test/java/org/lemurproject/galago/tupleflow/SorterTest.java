package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.tupleflow.Sorter;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.NullProcessor;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.FakeParameters;
import junit.framework.*;

/**
 *
 * @author trevor
 */
public class SorterTest extends TestCase {
    public SorterTest(String testName) {
        super(testName);
    }

    public void testGetInputClass() {
        Sorter instance = new Sorter(new FakeType().getOrder("+document", "+length"));

        Parameters p = new Parameters();
        p.set("class", FakeType.class.toString());
        String expResult = FakeType.class.toString();
        String result = Sorter.getInputClass(new FakeParameters(p));
        assertEquals(expResult, result);
    }

    public void testGetOutputClass() {
        Sorter instance = new Sorter(new FakeType().getOrder("+value"));

        Parameters p = new Parameters();
        p.set("class", FakeType.class.toString());
        String expResult = FakeType.class.toString();
        String result = Sorter.getOutputClass(new FakeParameters(p));
        assertEquals(expResult, result);
    }

    public void testProcess() throws Exception {
        FakeType object = new FakeType();
        Sorter instance = new Sorter(new FakeType().getOrder("+document", "+length"));

        instance.process(object);
    }

    public void testClose() throws Exception {
        FakeType object = new FakeType();
        Sorter instance = new Sorter(new FakeType().getOrder("+document", "+length"));

        instance.setProcessor(new NullProcessor(FakeType.class));
        instance.process(object);
        instance.close();
    }

    public void testSetProcessor() throws IncompatibleProcessorException {
        Sorter instance = new Sorter(new FakeType().getOrder("+document", "+length"));

        instance.setProcessor(new NullProcessor(FakeType.class));
    }
}
