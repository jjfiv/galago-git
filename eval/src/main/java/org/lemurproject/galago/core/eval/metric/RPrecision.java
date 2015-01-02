/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

/**
 * Returns the precision at the rank equal to the total number of
 * relevant documents retrieved.  This method is equivalent to
 * precision(relevantDocuments().size()).
 * 
 * @author trevor, sjh
 */
public class RPrecision extends QueryEvaluator {

  public RPrecision(String metric) {
    super(metric);
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    int relevantCount = judgments.getRelevantJudgmentCount();
    int retrievedCount = resultList.size();

    if (relevantCount > retrievedCount) {
      return 0;
    }

    return (new Precision(relevantCount)).evaluate(resultList, judgments);
  }
}
