/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.compare;

import org.lemurproject.galago.core.eval.stat.Stat;

/**
 *
 * @author trevor, sjh
 */
class SignTest extends QuerySetComparator {

  double boost;

  public SignTest(String testName) {
    super(testName);
    this.boost = 1.0;
  }

  public SignTest(String metric, double boost) {
    super(metric);
    this.boost = boost;
  }

  @Override
  public double evaluate(double[] baseline, double[] treatment) {
    int treatmentIsBetter = 0;
    int different = 0;

    for (int i = 0; i < treatment.length; i++) {
      double boostedBaseline = baseline[i] * boost;
      if (treatment[i] > boostedBaseline) {
        treatmentIsBetter++;
      }
      if (treatment[i] != boostedBaseline) {
        different++;
      }
    }

    double pvalue = Stat.binomialProb(0.5, different, treatmentIsBetter);
    return pvalue;
  }
}
