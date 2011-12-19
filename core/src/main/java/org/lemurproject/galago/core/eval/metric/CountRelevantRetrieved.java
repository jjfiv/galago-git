/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;
import org.lemurproject.galago.core.retrieval.ScoredDocument;

/**
 * Returns the number of relevant documents retrieved.
 * 
 * @author sjh
 */
public class CountRelevantRetrieved extends QueryEvaluator {

  double documentsRetrieved;

  // without a fixed point
  public CountRelevantRetrieved(String metric) {
    super(metric);
    if(metric.contains("@")){
      this.documentsRetrieved = Integer.parseInt(metric.split("@")[1]);
    } else {
      this.documentsRetrieved = Integer.MAX_VALUE;
    }
  }

  // with a fixed point
  public CountRelevantRetrieved(int documentsRetrieved) {
    super("num_rel_ret@"+documentsRetrieved);
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
    for (ScoredDocument doc : resultList.getIterator()) {
      if (doc.rank > documentsRetrieved) {
        return count;
      }
      if (judgments.isRelevant(doc.documentName)) {
        count++;
      }
    }
    return count;
  }
}
