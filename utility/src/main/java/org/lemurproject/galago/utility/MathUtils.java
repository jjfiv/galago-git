// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility;

import java.util.Arrays;

/**
 * Supplements Java's sucky math class.
 *
 * @author irmarc
 */
public class MathUtils {

  /** Can't instantiate Math. It's ALWAYS there. */
  private MathUtils() {
  }

  /** Multiplicative form from Wikipedia
	 * @author irmarc
	 */
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

  public static double logSumExp(double[] scores) {
    double[] weights = new double[scores.length];
    Arrays.fill(weights, 1.0);
    return weightedLogSumExp(weights, scores);
  }

  /**
   * Compute the log(geometric_mean( weight[0]*exp(score[0]) ...))
   * This is just the arithmetic mean with logspace scores...
   * http://en.wikipedia.org/wiki/Geometric_mean#Relationship_with_arithmetic_mean_of_logarithms
   */
  public static double logWeightedGeometricMean(double[] weights, double[] logScores) {
    return weightedArithmeticMean(weights, logScores);
  }

  /** Compute the weighted arithmetic mean */
  public static double weightedArithmeticMean(double[] weights, double[] scores) {
    if(weights.length == 0) return 0;
    if(weights.length != scores.length) throw new IllegalArgumentException("weights and scores must be of the same length");

    double sum = 0;
    for (int i = 0; i < scores.length; i++) {
      sum += weights[i]*scores[i];
    }
    return sum / ((double) weights.length);
  }

  /**
   * Compute the weighted geometric mean.
   */
  public static double weightedGeometricMean(double[] weights, double[] scores) {
    return 0.0;
  }

  /**
   * Computes the weighted average of scores: -> log( w0 * exp(score[0]) + w1 *
   * exp(score[1]) + w1 * exp(score[2]) + .. )
   *
   * to avoid rounding errors, we compute the equivalent expression:
   *
   * returns: maxScore + log( w0 * exp(score[0] - max) + w1 * exp(score[1] -
   * max) + w2 * exp(score[2] - max) + .. )
   */
  public static double weightedLogSumExp(double[] weights, double[] scores) {
    if(scores.length == 0){
      throw new RuntimeException( "weightedLogSumExp was called with a zero length array of scores." );
    }
    
    // find max value - this score will dominate the final score
    double max = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < scores.length; i++) {
      max = Math.max(scores[i], max);
    }

    double sum = 0;
    for (int i = 0; i < scores.length; i++) {
      sum += weights[i] * java.lang.Math.exp(scores[i] - max);
    }
    sum = max + java.lang.Math.log(sum);

    return sum;
  }
}
