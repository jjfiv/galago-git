package org.lemurproject.galago.tupleflow;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
@SuppressWarnings("unchecked")
public class OrderedCombinerTest {
  @Test
  public void testGetOutputClass() {
    OrderedCombiner instance = new OrderedCombiner<>(new TypeReader[0], new FakeType().getOrder("+value"));

    Class expResult = FakeType.class;
    Class result = instance.getOutputClass();
    assertEquals(expResult, result);
  }

  @Test
  public void testRun() throws Exception {
    OrderedCombiner instance = new OrderedCombiner<>(new TypeReader[0], new FakeType().getOrder("+value"));
    instance.run();
  }
}
