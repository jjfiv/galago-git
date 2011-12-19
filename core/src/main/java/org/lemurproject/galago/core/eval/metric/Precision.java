/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

/**
 * Returns the precision of the retrieval at a given number of documents retrieved.
 * The precision is the number of relevant documents retrieved
 * divided by the total number of documents retrieved.
 *
 * @param documentsRetrieved The evaluation rank.
 * 
 * @author trevor, sjh
 */
public class Precision extends QueryEvaluator {

  CountRelevantRetrieved relevantRetrieved;
  int documentsRetrieved;

  public Precision(String metric, int documentsRetrieved) {
    super(metric);
    this.documentsRetrieved = documentsRetrieved;
    this.relevantRetrieved = new CountRelevantRetrieved(documentsRetrieved);
  }

  public Precision(String metric) {
    super(metric);
    documentsRetrieved = Integer.MAX_VALUE;
    this.relevantRetrieved = new CountRelevantRetrieved(documentsRetrieved);
  }

  public Precision(int documentsRetrieved) {
    super("P" + documentsRetrieved);
    this.documentsRetrieved = documentsRetrieved;
    this.relevantRetrieved = new CountRelevantRetrieved(documentsRetrieved);
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    return relevantRetrieved.evaluate(resultList, judgments) / (double) documentsRetrieved;
  }
}