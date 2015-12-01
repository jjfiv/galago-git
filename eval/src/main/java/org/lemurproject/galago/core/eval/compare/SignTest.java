/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.compare;

//import org.lemurproject.galago.core.eval.stat.Stat;
import org.apache.commons.math3.stat.inference.AlternativeHypothesis;
import org.apache.commons.math3.stat.inference.BinomialTest;


/**
 *
 * @author trevor, sjh
 */
class SignTest extends QuerySetComparator {
  public static double tolerance = 0.0000001;
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
      if (Math.abs(treatment[i] - boostedBaseline) > tolerance) {
        different++;
      }
    }

    double pvalue;
    //pvalue = Stat.binomialProb(0.5, different, treatmentIsBetter);

    //- Use Apache Commons Math3 Binomial (Sign) Test directly.  Assume
    //  generic, two-sided test.  For one-sided test, change to
    //  AlternativeHypothesis GREATER_THAN (right-sided) or
    //  LESS_THAN (left-sided) tests.
    BinomialTest bt = new BinomialTest ();
    pvalue = bt.binomialTest (different, treatmentIsBetter, 0.5,
			      AlternativeHypothesis.TWO_SIDED);
    return pvalue;
  }
}
