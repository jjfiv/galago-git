// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.util;

/**
 * Supplements Java's sucky math class.
 *
 * @author irmarc
 */
public class Math {

    // Can't instantiate Math. It's ALWAYS there.
    private Math() {
    }

    // Can't believe I'm implementing this.
    // multiplicative form from Wikipedia -- irmarc
    public static long binomialCoeff(int n, int k) {
        if (n <= k) {
            return 1;
        }
        int c;
        if (k > n - k) { // take advantage of symmetry
            k = n - k;
        }
        c = 1;
        for (int i = 0; i < k; i++) {
            c *= (n - i);
            c /= (i + 1);

        }
        return c;
    }
}
