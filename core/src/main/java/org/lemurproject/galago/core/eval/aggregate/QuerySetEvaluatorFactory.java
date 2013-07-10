/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.aggregate;

import org.lemurproject.galago.core.eval.metric.QueryEvaluator;
import org.lemurproject.galago.core.eval.metric.QueryEvaluatorFactory;

import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Factory for QuerySetEvaluators
 *
 * @author sjh
 */
public class QuerySetEvaluatorFactory {

  public static QuerySetEvaluator instance(String metric, Parameters p) {

    String lowerMetric = metric.toLowerCase();
    // sum metrics:
    if (lowerMetric.equals("num_ret")
            || lowerMetric.equals("num_rel")
            || lowerMetric.startsWith("num_rel_ret")
            || lowerMetric.startsWith("num_unjug_ret")) {
      QueryEvaluator evalFn = QueryEvaluatorFactory.instance(metric, p);
      return new Sum(metric, evalFn);

      // mean metrics
    } else if (
            lowerMetric.equals("frac_unjug_ret")
            || lowerMetric.equals("map")
            || lowerMetric.equals("averagePrecision")
            || lowerMetric.equals("bpref")
            || lowerMetric.equals("r-prec")
            || lowerMetric.equals("rPrecision")
            || lowerMetric.equals("recip_rank")
            || lowerMetric.startsWith("ndcg")
            || lowerMetric.startsWith("err")
            || lowerMetric.startsWith("p")
            || lowerMetric.startsWith("r")
            || lowerMetric.startsWith("mdfa")) {
      QueryEvaluator evalFn = QueryEvaluatorFactory.instance(metric, p);
      return new Mean(metric, evalFn);

      // geometric mean metrics
    } else if (lowerMetric.equals("gmap")) {
      QueryEvaluator evalFn = QueryEvaluatorFactory.instance(metric, p);
      return new GeometricMean(metric, evalFn);

      // otherwise unknown
    } else {
      throw new RuntimeException("Evaluation metric " + metric + " is unknown to QuerySetEvaluator.");
    }
  }
}
