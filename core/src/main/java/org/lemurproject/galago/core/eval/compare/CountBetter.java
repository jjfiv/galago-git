package org.lemurproject.galago.core.eval.compare;

/**
 *
 * @author trevor, sjh
 */
class CountBetter extends QuerySetComparator {

  boolean useBaseline;

  public CountBetter(String testName, boolean useBaseline) {
    super(testName);
    this.useBaseline = useBaseline;
  }

  @Override
  public double evaluate(double[] baseline, double[] treatment) {
    double[] target = (useBaseline) ? baseline : treatment;
    double[] other = (useBaseline) ? treatment : baseline;

    double count = 0;
    for (int i = 0; i < target.length; i++) {
      if (target[i] > other[i]) {
        count++;
      }
    }

    return count;
  }
}
