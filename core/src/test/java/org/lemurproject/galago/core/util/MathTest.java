// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.util;

import org.lemurproject.galago.core.util.Math;
import junit.framework.TestCase;

/**
 *
 * @author marc
 */
public class MathTest extends TestCase {

    public void testBinomialCoeff() throws Exception {
        // choose(7, 3) = 35
        long result = Math.binomialCoeff(7, 3);
        assertEquals(35, result);

        // Check when k > n-k
        // choose(5,3) = 10
        result = Math.binomialCoeff(5, 3);
        assertEquals(result, 10);
    }
}
