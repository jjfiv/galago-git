/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.aggregate;

import org.lemurproject.galago.core.eval.metric.QueryEvaluator;
import org.lemurproject.galago.core.eval.metric.QueryEvaluatorFactory;

/**
 * Factory for QuerySetEvaluators
 *
 * @author sjh
 */
public class QuerySetEvaluatorFactory {

  public static QuerySetEvaluator instance(String metric) {

    String lowerMetric = metric.toLowerCase();
    // sum metrics:
    if (lowerMetric.equals("num_ret")
            || lowerMetric.equals("num_rel")
            || lowerMetric.equals("num_rel_ret")) {
      QueryEvaluator evalFn = QueryEvaluatorFactory.instance(metric);
      return new Sum(metric, evalFn);

      // mean metrics
    } else if (lowerMetric.equals("map")
            || lowerMetric.equals("averagePrecision")
            || lowerMetric.equals("bpref")
            || lowerMetric.equals("r-prec")
            || lowerMetric.equals("rPrecision")
            || lowerMetric.equals("recip_rank")
            || lowerMetric.startsWith("ndcg")
            || lowerMetric.startsWith("err")
            || lowerMetric.startsWith("p")
            || lowerMetric.startsWith("r")) {
      QueryEvaluator evalFn = QueryEvaluatorFactory.instance(metric);
      return new Mean(metric, evalFn);

      // geometric mean metrics
    } else if (lowerMetric.equals("gmap")) {
      QueryEvaluator evalFn = QueryEvaluatorFactory.instance(metric);
      return new GeometricMean(metric, evalFn);

      // otherwise unknown
    } else {
      throw new RuntimeException("Evaluation metric " + metric + " is unknown to QuerySetEvaluator.");
    }
  }
}
