/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;
import org.lemurproject.galago.core.retrieval.ScoredDocument;

/**
 * Returns the average precision of the query.
 *
 * Suppose the precision is evaluated once at the rank of
 * each relevant document in the retrieval.  If a document is
 * not retrieved, we assume that it was retrieved at rank infinity.
 * The mean of all these precision values is the average precision.
 *
 * @author trevor, sjh
 */
public class AveragePrecision extends QueryEvaluator {

  public AveragePrecision(String metric) {
    super(metric);
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    double sumPrecision = 0;
    int relevantCount = 0;

    for (ScoredDocument doc : resultList.getIterator()) {
      if (judgments.isRelevant(doc.documentName)) {
        relevantCount++;
        sumPrecision += relevantCount / (double) doc.rank;
      }
    }
    return (double) sumPrecision / judgments.getRelevantJudgmentCount();
  }
}
