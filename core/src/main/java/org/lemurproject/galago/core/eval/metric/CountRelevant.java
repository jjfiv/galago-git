/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

/**
 * returns the total number of relevant documents in the judgment set
 *
 * @author sjh
 */
public class CountRelevant extends QueryEvaluator {

  public CountRelevant(String metric) {
    super(metric);
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    return judgments.getRelevantJudgmentCount();
  }
}
