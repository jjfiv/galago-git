package org.lemurproject.galago.core.eval.compare;

/**
 *
 * @author trevor, sjh
 */
class CountEqual extends QuerySetComparator {

  public CountEqual(String testName) {
    super(testName);
  }

  @Override
  public double evaluate(double[] baseline, double[] treatment) {
    double count = 0;
    for (int i = 0; i < baseline.length; i++) {
      if (baseline[i] == treatment[i]) {
        count++;
      }
    }
    return count;
  }
}
