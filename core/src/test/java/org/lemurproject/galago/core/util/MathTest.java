// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.util;


import org.junit.Test;
import org.lemurproject.galago.utility.MathUtils;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author marc
 */
public class MathTest {

  @Test
  public void testBinomialCoeff() throws Exception {
    // choose(7, 3) = 35
    long result = MathUtils.binomialCoeff(7, 3);
    assertEquals(35, result);

    // Check when k > n-k
    // choose(5,3) = 10
    result = MathUtils.binomialCoeff(5, 3);
    assertEquals(result, 10);
  }
}
