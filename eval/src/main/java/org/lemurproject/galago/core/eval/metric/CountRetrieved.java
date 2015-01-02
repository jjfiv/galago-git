/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

/**
 * Returns the number of documents retrieved.
 *
 * @author sjh
 */
public class CountRetrieved extends QueryEvaluator {

  public CountRetrieved(String metric){
    super(metric);
  }
  
  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    return resultList.size();
  }
}
