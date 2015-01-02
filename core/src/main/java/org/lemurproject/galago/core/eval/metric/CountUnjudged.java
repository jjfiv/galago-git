/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.EvalDoc;
import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

/**
 * returns the total number of unjudged documents in the result list
 *
 * @author sjh
 */
public class CountUnjudged extends QueryEvaluator {


  double documentsRetrieved;

  // without a fixed point
  public CountUnjudged(String metric) {
    super(metric);
    if(metric.contains("@")){
      this.documentsRetrieved = Integer.parseInt(metric.split("@")[1]);
    } else {
      this.documentsRetrieved = Integer.MAX_VALUE;
    }
  }

  // with a fixed point
  public CountUnjudged(int documentsRetrieved) {
    super("num_unjug_ret@"+documentsRetrieved);
    this.documentsRetrieved = documentsRetrieved;
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    return relevantRetrieved(resultList, judgments);
  }

  /**
   * The number of relevant documents retrieved at a particular
   * rank.  This is equivalent to <tt>n * precision(n)</tt>.
   */
  private int relevantRetrieved(QueryResults resultList, QueryJudgments judgments) {
    int count = 0;
    for (EvalDoc doc : resultList.getIterator()) {
      // assumes that documents iterate in increasing rank order.
      if (doc.getRank() > documentsRetrieved) {
        return count;
      }
      if (!judgments.isJudged(doc.getName())) {
        count++;
      }
    }
    return count;
  }
}
