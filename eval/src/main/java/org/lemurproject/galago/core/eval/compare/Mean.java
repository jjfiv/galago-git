/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.compare;

/**
 *
 * @author trevor, sjh
 */
class Mean extends QuerySetComparator {

  boolean useBaseline;

  public Mean(String testName, boolean useBaseline) {
    super(testName);
    this.useBaseline = useBaseline;
  }

  @Override
  public double evaluate(double[] baseline, double[] treatment) {
    double[] target = (useBaseline) ? baseline : treatment;
    return mean(target);
  }
}
