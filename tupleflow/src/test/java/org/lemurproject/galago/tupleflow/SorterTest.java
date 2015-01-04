package org.lemurproject.galago.tupleflow;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.runtime.NullProcessor;
import org.lemurproject.galago.utility.Parameters;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class SorterTest {
  @Test
  public void testGetInputClass() {
    Parameters p = Parameters.create();
    p.set("class", FakeType.class.toString());
    String expResult = FakeType.class.toString();
    String result = Sorter.getInputClass(new FakeParameters(p));
    assertEquals(expResult, result);
  }

  @Test
  public void testGetOutputClass() {
    Parameters p = Parameters.create();
    p.set("class", FakeType.class.toString());
    String expResult = FakeType.class.toString();
    String result = Sorter.getOutputClass(new FakeParameters(p));
    assertEquals(expResult, result);
  }

  @Test
  public void testProcess() throws Exception {
    FakeType object = new FakeType();
    Sorter<FakeType> instance = new Sorter<FakeType>(new FakeType().getOrder("+document", "+length"));
    instance.process(object);
  }

  @Test
  public void testClose() throws Exception {
    FakeType object = new FakeType();
    Sorter<FakeType> instance = new Sorter<FakeType>(new FakeType().getOrder("+document", "+length"));

    instance.setProcessor(new NullProcessor<FakeType>(FakeType.class));
    instance.process(object);
    instance.close();
  }

  @Test
  public void testSetProcessor() throws IncompatibleProcessorException {
    Sorter<FakeType> instance = new Sorter<FakeType>(new FakeType().getOrder("+document", "+length"));
    instance.setProcessor(new NullProcessor<FakeType>(FakeType.class));
  }
}
