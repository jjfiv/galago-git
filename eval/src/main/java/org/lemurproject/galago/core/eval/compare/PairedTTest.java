/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.compare;

//import org.lemurproject.galago.core.eval.stat.Stat;
import org.apache.commons.math3.stat.inference.TTest;


/**
 *
 * @author trevor, sjh
 */
public class PairedTTest extends QuerySetComparator {

  double boost;

  public PairedTTest(String testName) {
    super(testName);
    this.boost = 1.0;
  }

  public PairedTTest(String metric, double boost) {
    super(metric);
    this.boost = boost;
  }

  @Override
  public double evaluate(double[] baseline, double[] treatment) {

    double[] boostedBaseline = multiply(baseline, boost);

    /*
    double sampleSum = 0;
    double sampleSumSquares = 0;
    int n = boostedBaseline.length;

    for (int i = 0; i < baseline.length; i++) {
      double delta = treatment[i] - boostedBaseline[i];
      sampleSum += delta;
      sampleSumSquares += delta * delta;
    }

    double sampleVariance = sampleSumSquares / (n - 1);
    double sampleMean = sampleSum / baseline.length;

    double sampleDeviation = Math.sqrt(sampleVariance);
    double meanDeviation = sampleDeviation / Math.sqrt(n);
    double t = sampleMean / meanDeviation;

    return 1.0 - Stat.studentTProb(t, n - 1);
    */

    //- Use Apache Commons Math3 directly for t-test p-value calculation
    TTest tt = new TTest();
    double pval = tt.tTest (boostedBaseline, treatment);

    return 1.0 - pval;
  }
}
