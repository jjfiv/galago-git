/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.EvalDoc;
import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

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
    double sumPrecision = 0.0;
    int relevantCount = 0;
    
    for (EvalDoc doc : resultList.getIterator()) {
      if (judgments.isRelevant(doc.getName())) {
        relevantCount++;
        sumPrecision += relevantCount / (double) doc.getRank();
      }
    }

    if (judgments.getRelevantJudgmentCount() > 0) {
      return sumPrecision / (double) judgments.getRelevantJudgmentCount();
    }
    // if there are no relevant documents, 
    // the average is artificially defined as zero, to mimic trec_eval
    // Really, the output is NaN, or the query should be ignored.
    return 0.0;
  }
}
