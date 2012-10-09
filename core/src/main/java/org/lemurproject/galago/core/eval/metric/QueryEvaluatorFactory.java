/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Factory for QueryEvaluators
 *
 * @author sjh
 */
public class QueryEvaluatorFactory {

  // static functions allow the creation of QueryEvaluators based on strings
  public static QueryEvaluator instance(String metric, Parameters p) {

    String lowerMetric = metric.toLowerCase();
    
    // these metrics may not be parametized
    if (lowerMetric.equals("num_ret")) {
      return new CountRetrieved(metric);
    } else if (lowerMetric.equals("num_rel")) {
      return new CountRelevant(metric);
    } else if (lowerMetric.equals("num_rel_ret")) {
      return new CountRelevantRetrieved(metric);
    } else if (lowerMetric.equals("map")
            || lowerMetric.equals("averagePrecision")) {
      return new AveragePrecision(metric);
    } else if (lowerMetric.equals("r-prec")
            || lowerMetric.equals("rprecision")) {
      return new RPrecision(metric);
    } else if (lowerMetric.equals("bpref")) {
      return new BinaryPreference(metric);
    } else if (lowerMetric.equals("recip_rank")) {
      return new ReciprocalRank(metric);
    } else if (lowerMetric.equals("p")) {
      return new Precision(metric);
    } else if (lowerMetric.equals("r")) {
      return new Recall(metric);
    } else if (lowerMetric.equals("ndcg")) {
      return new NormalizedDiscountedCumulativeGain(metric);
    } else if (lowerMetric.equals("err")) {
      return new ExpectedReciprocalRank(metric);
        
    // these may be parametized (e.g. P5, R10, ndcg20, ...)
    } else if (lowerMetric.startsWith("p")) {
      int documentLimit = Integer.parseInt(lowerMetric.replace("p", ""));
      return new Precision(metric, documentLimit);
    } else if (lowerMetric.startsWith("r")) {
      int documentLimit = Integer.parseInt(lowerMetric.replace("r", ""));
      return new Recall(metric, documentLimit);
    } else if (lowerMetric.startsWith("ndcg")) {
      int documentLimit = Integer.parseInt(lowerMetric.replace("ndcg", ""));
      return new NormalizedDiscountedCumulativeGain(metric, documentLimit);
    } else if (lowerMetric.startsWith("err")) {
      int documentLimit = Integer.parseInt(lowerMetric.replace("err", ""));
      return new ExpectedReciprocalRank(metric, documentLimit);
      
    } else if (lowerMetric.startsWith("mdfa")) {
        int falseAlarmRate = Integer.parseInt(lowerMetric.replace("mdfa", ""));
        double rate = falseAlarmRate / (double) 100;
        long collectionSize = p.getLong("collectionSize");
        return new MissDetectionFalseAlarm(metric, collectionSize, rate);
      
    // otherwise we don't know which metric to use.
    } else {
      throw new RuntimeException("Evaluation metric " + metric + " is unknown to QueryEvaluator.");
    }
  }
}
