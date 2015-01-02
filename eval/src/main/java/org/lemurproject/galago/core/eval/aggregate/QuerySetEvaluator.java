/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.aggregate;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;
import org.lemurproject.galago.core.eval.QuerySetEvaluation;
import org.lemurproject.galago.core.eval.QuerySetJudgments;
import org.lemurproject.galago.core.eval.QuerySetResults;
import org.lemurproject.galago.core.eval.metric.QueryEvaluator;

/**
 * QuerySetEvaluators aggregate implemented 
 * evaluation metrics across a set of queries.
 * 
 * Examples include sum, mean and geometric mean.
 * 
 * input: a set of ranked-lists, one for each query
 * input: a set of relevance-judgment-lists, one for each query
 * output: aggregated value of a retrieval metric
 *
 * @author sjh
 */
public abstract class QuerySetEvaluator {

  protected String metric;
  protected final QueryEvaluator evaluator;

  public QuerySetEvaluator(String metric, QueryEvaluator evaluator) {
    this.metric = metric;
    this.evaluator = evaluator;
  }

  public String getMetric() {
    return metric;
  }

  // aggregate metric
  public abstract double evaluate(QuerySetResults querySet, QuerySetJudgments judgmentSet);

  // single query evaluator - passes parameters through
  public double evaluate(QueryResults querySet, QueryJudgments judgmentSet) {
    // if we have no results or no judgments -- eval returns NaN
    if (querySet != null && judgmentSet != null) {
      return evaluator.evaluate(querySet, judgmentSet);
    } else {
      return Double.NaN;
    }
  }

  // query set evaluator - passes each parameter through - returns all results
  public QuerySetEvaluation evaluateSet(QuerySetResults querySet, QuerySetJudgments judgmentSet) {
    QuerySetEvaluation evaluation = new QuerySetEvaluation();
    for (String query : querySet.getQueryIterator()) {
      QueryResults qres = querySet.get(query);
      QueryJudgments qjudge = judgmentSet.get(query);

      if (qres != null && qjudge != null ) {
        double eval = evaluator.evaluate(querySet.get(query), judgmentSet.get(query));
        evaluation.add(query, eval);
      }

    }
    return evaluation;
  }
}
