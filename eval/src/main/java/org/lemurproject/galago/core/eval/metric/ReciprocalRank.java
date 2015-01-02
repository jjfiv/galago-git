/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.EvalDoc;
import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

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
    for (EvalDoc doc : resultList.getIterator()) {
      if (judgments != null && judgments.isRelevant(doc.getName())) {
        int firstRelevantDocumentRank = doc.getRank();
        return 1.0 / (double) firstRelevantDocumentRank;
      }
    }
    return 0;
  }
}
