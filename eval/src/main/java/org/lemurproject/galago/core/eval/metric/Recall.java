/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

/**
 * Returns the recall of the retrieval at a given number of documents retrieved.
 * The recall is the number of relevant documents retrieved
 * divided by the total number of relevant documents for the query.
 *
 * @param documentsRetrieved The evaluation rank.
 * @author trevor, sjh
 */
public class Recall extends QueryEvaluator {

  CountRelevantRetrieved relevantRetrieved;
  int documentsRetrieved;

  public Recall(String metric, int documentsRetrieved) {
    super(metric);
    this.documentsRetrieved = documentsRetrieved;
    this.relevantRetrieved = new CountRelevantRetrieved(documentsRetrieved);
  }

  public Recall(String metric) {
    super(metric);
    documentsRetrieved = Integer.MAX_VALUE;
    this.relevantRetrieved = new CountRelevantRetrieved(documentsRetrieved);
  }

  public Recall(int documentsRetrieved) {
    super("R" + documentsRetrieved);
    this.documentsRetrieved = documentsRetrieved;
    this.relevantRetrieved = new CountRelevantRetrieved(documentsRetrieved);
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    return relevantRetrieved.evaluate(resultList, judgments) / judgments.getRelevantJudgmentCount();
  }
}
