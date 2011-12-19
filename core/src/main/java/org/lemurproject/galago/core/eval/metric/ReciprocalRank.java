/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;
import org.lemurproject.galago.core.retrieval.ScoredDocument;

/**
 * Returns the reciprocal of the rank of the first relevant document
 * retrieved, or zero if no relevant documents were retrieved.
 * 
 * @author trevor, sjh
 */
public class ReciprocalRank extends QueryEvaluator {

  public ReciprocalRank(String metric) {
    super(metric);
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    for (ScoredDocument doc : resultList.getIterator()) {
      if (judgments != null && judgments.isRelevant(doc.documentName)) {
        int firstRelevantDocumentRank = doc.rank;
        return 1.0 / (double) firstRelevantDocumentRank;
      }
    }
    return 0;
  }
}
