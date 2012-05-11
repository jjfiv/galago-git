/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.compare;

import java.util.Random;

/**
 *
 * @author trevor, sjh
 */
class RandomizedTest extends QuerySetComparator {

  double boost;

  public RandomizedTest(String testName) {
    super(testName);
    this.boost = 1.0;
  }

  public RandomizedTest(String metric, double boost) {
    super(metric);
    this.boost = boost;
  }

  @Override
  public double evaluate(double[] baseline, double[] treatment) {
    double[] boostedBaseline = multiply(baseline, boost);
    double baseMean = mean(boostedBaseline);
    double treatmentMean = mean(treatment);
    double difference = treatmentMean - baseMean;
    int batch = 10000;

    final int maxIterationsWithoutMatch = 1000000;
    long iterations = 0;
    long matches = 0;

    double[] leftSample = new double[boostedBaseline.length];
    double[] rightSample = new double[boostedBaseline.length];
    Random random = new Random();
    double pValue = 0.0;

    while (true) {
      for (int i = 0; i < batch; i++) {
        // create a sample from both distributions
        for (int j = 0; j < boostedBaseline.length; j++) {
          if (random.nextBoolean()) {
            leftSample[j] = boostedBaseline[j];
            rightSample[j] = treatment[j];
          } else {
            leftSample[j] = treatment[j];
            rightSample[j] = boostedBaseline[j];
          }
        }

        double sampleDifference = mean(leftSample) - mean(rightSample);

        if (difference <= sampleDifference) {
          matches++;
        }
      }

      iterations += batch;

      // this is the current p-value estimate
      pValue = (double) matches / (double) iterations;

      // if we still haven't found a match, keep looking
      if (matches == 0) {
        if (iterations < maxIterationsWithoutMatch) {
          continue;
        } else {
          break;
        }
      }

      // this is our accepted level of deviation in the p-value; we require:
      //      - accuracy at the fourth decimal place, and
      //      - less than 5% error in the p-value, or
      //      - accuracy at the sixth decimal place.

      double maxDeviation = Math.max(0.0000005 / pValue, Math.min(0.00005 / pValue, 0.05));

      // this estimate is derived in Efron and Tibshirani, p.209.
      // this is the estimated number of iterations necessary for convergence, given
      // our current p-value estimate.
      double estimatedIterations = Math.sqrt(pValue * (1.0 - pValue)) / maxDeviation;

      if (estimatedIterations < iterations) {
        break;
      }
    }

    return pValue;
  }
}
