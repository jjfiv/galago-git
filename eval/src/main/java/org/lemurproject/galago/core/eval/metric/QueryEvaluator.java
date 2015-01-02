/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

/**
 * A retrieval evaluator object computes one of a variety of standard
 * information retrieval metrics commonly used in TREC 
 * for a particular query
 * 
 * Examples include: binary preference (BPREF), average precision (AP),
 * and standard precision and recall.
 *
 * input: a ranked-list from the execution of a query, 
 * input: the corresponding relevance-judgment-list for the same query.
 * output: evaluation of the query results.
 * 
 * @author sjh
 */
public abstract class QueryEvaluator {

  protected String metric;

  public QueryEvaluator(String metric) {
    this.metric = metric;
  }
  
  public String getMetric() {
    return metric;
  }

  public abstract double evaluate(QueryResults resultList, QueryJudgments judgments);
}
