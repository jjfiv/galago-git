/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;
import org.lemurproject.galago.core.retrieval.ScoredDocument;

/**
 * returns the fraction of unjudged documents in the result list
 *
 * @author sjh
 */
public class FractionUnjudged extends QueryEvaluator {


  double documentsRetrieved;

  // without a fixed point
  public FractionUnjudged(String metric) {
    super(metric);
    if(metric.contains("@")){
      this.documentsRetrieved = Integer.parseInt(metric.split("@")[1]);
    } else {
      this.documentsRetrieved = Integer.MAX_VALUE;
    }
  }

  // with a fixed point
  public FractionUnjudged(int documentsRetrieved) {
    super("frac_unjug_ret@"+documentsRetrieved);
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
  private double relevantRetrieved(QueryResults resultList, QueryJudgments judgments) {
    int count = 0;
    for (ScoredDocument doc : resultList.getIterator()) {
      // assumes that documents iterate in increasing rank order.
      if (doc.rank > documentsRetrieved) {
        break;
      }
      if (!judgments.isJudged(doc.documentName)) {
        count++;
      }
    }
    double retrieved = (documentsRetrieved < resultList.size())? documentsRetrieved: resultList.size();
    return count / retrieved;
  }
}
