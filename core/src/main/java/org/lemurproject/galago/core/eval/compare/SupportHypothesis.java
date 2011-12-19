/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.compare;

/**
 *
 * @author trevor, sjh
 */
public class SupportHypothesis extends QuerySetComparator {

  private String comparison;
  private double pvalue;

  public SupportHypothesis(String testName, String comparison, double pvalue) {
    super(testName);
    this.comparison = comparison;
    this.pvalue = pvalue;
  }

  @Override
  public double evaluate(double[] baseline, double[] treatment) {
    return supportedHypothesis(baseline, treatment);
  }

  private double supportedHypothesis(double[] baseline, double[] treatment) {
    double currentBoost = 1.0;
    double currentPvalue = (QuerySetComparatorFactory.instance(comparison, currentBoost)).evaluate(baseline, treatment);
    double lastBoost = 1.0;
    double lastPvalue = currentPvalue;
    int iterations = 0;

    // search until we find an interval
    while ((lastPvalue < pvalue) == (currentPvalue < pvalue)) {
      double nextBoost = currentBoost;

      if (currentPvalue < pvalue) {
        nextBoost *= 1.05;
      } else if (currentPvalue > pvalue) {
        nextBoost *= 0.95;
      }

      double nextPvalue = (QuerySetComparatorFactory.instance(comparison, nextBoost)).evaluate(baseline, treatment);

      lastBoost = currentBoost;
      lastPvalue = currentPvalue;
      currentBoost = nextBoost;
      currentPvalue = nextPvalue;

      iterations++;

      if (iterations > 50) {
        return 0;
      }
    }

    // now we have an interval to search in
    double lowBoost = Math.min(lastBoost, currentBoost);
    double highBoost = Math.max(lastBoost, currentBoost);

    while (highBoost - lowBoost > 0.00005) {
      double middleBoost = (highBoost + lowBoost) / 2;
      currentPvalue = (QuerySetComparatorFactory.instance(comparison, middleBoost)).evaluate(baseline, treatment);

      if (currentPvalue > pvalue) {
        highBoost = middleBoost;
      } else {
        lowBoost = middleBoost;
      }

      iterations++;

      if (iterations > 100) {
        return 0;
      }
    }

    return lowBoost;
  }
}
