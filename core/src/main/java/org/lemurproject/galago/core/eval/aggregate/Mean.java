/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.aggregate;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;
import org.lemurproject.galago.core.eval.QuerySetJudgments;
import org.lemurproject.galago.core.eval.QuerySetResults;
import org.lemurproject.galago.core.eval.metric.QueryEvaluator;

/**
 * Returns the mean metric value for the set of query result-lists.
 *
 * @author sjh
 */
public class Mean extends QuerySetEvaluator {

  public Mean(String metric, QueryEvaluator evaluator) {
    super(metric, evaluator);
  }

  @Override
  public double evaluate(QuerySetResults querySet, QuerySetJudgments judgmentSet) {
    double sum = 0;
    double count = 0;
    for (String query : querySet.getQueryIterator()) {
      QueryResults qres = querySet.get(query);
      QueryJudgments qjudge = judgmentSet.get(query);
      if (qres != null && qjudge != null) {
        double eval = evaluator.evaluate(qres, qjudge);
        if (!Double.isNaN(eval)) {
          sum += eval;
        }
        count += 1;
      }
    }
    return sum / count;
  }
}
