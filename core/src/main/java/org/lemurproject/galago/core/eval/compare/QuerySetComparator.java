/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.compare;

import org.lemurproject.galago.core.eval.QuerySetEvaluation;

/**
 * QuerySetComparators perform statistical tests to help determine
 * how different the results of one retrieval model are from another
 * 
 * Order of retrieval models is important - the first is considered
 * the baseline model, the second is considered the treatment.
 * 
 * @author sjh
 */
public abstract class QuerySetComparator {

  String testName;

  public QuerySetComparator(String testName) {
    this.testName = testName;
  }

  public String getTestName() {
    return testName;
  }

  public double evaluate(QuerySetEvaluation baseline, QuerySetEvaluation treatment) {
    // create aligned baseline and treatment arrays
    assert (baseline.size() == treatment.size());

    double[] base = new double[baseline.size()];
    double[] treat = new double[treatment.size()];
    int i = 0;
    for (String q : baseline.getIterator()) {
      base[i] = baseline.get(q);
      treat[i] = treatment.get(q);
      i++;
    }
    return evaluate(base, treat);
  }

  public abstract double evaluate(double[] baseline, double[] treatment);

  protected double mean(double[] numbers) {
    double sum = 0;
    for (int i = 0; i < numbers.length; i++) {
      if (!Double.isNaN(numbers[i])) {
        sum += numbers[i];
      }
    }

    return sum / (double) numbers.length;
  }

  protected static double[] multiply(double[] numbers, double boost) {
    double[] result = new double[numbers.length];

    for (int i = 0; i < result.length; i++) {
      result[i] = numbers[i] * boost;
    }

    return result;
  }
}
