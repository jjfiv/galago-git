/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.EvalDoc;
import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

/** 
 * <p>Expected Reciprocal Rank</p>
 *
 * "Expected Reciprocal Rank", by Chapelle et al. 2009, CIKM.
 *
 * Score = \sum_{i=1}^n (1/i * R(y_i) \proj_{j=1}^{i-1} (1 - R(y_i)))
 *
 * Where R(y) = (2^y - 1) / (16)
 * 
 * 
 * 
 * @author sjh
 */
public class ExpectedReciprocalRank extends QueryEvaluator {
  // this is the maximum value a judgment may take (TREC)
  int maxJudgment = 4;
  int documentsRetrieved;

  public ExpectedReciprocalRank(String metric, int documentsRetrieved) {
    super(metric);
    this.documentsRetrieved = documentsRetrieved;
  }

  public ExpectedReciprocalRank(String metric) {
    super(metric);
    documentsRetrieved = Integer.MAX_VALUE;
  }

  public ExpectedReciprocalRank(int documentsRetrieved) {
    super("err" + documentsRetrieved);
    this.documentsRetrieved = documentsRetrieved;
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    // compute err:
    double[] documentJudgments = new double[resultList.size()];
    int index = 0;
    for (EvalDoc doc : resultList.getIterator()) {
      // document judgments must be positive
      documentJudgments[index] = 0;
      if (judgments.get(doc.getName()) > 0) {
        documentJudgments[index] = judgments.get(doc.getName());
      }
      index++;
    }

    double err = computeERR(documentJudgments);


    return err;
  }

  /**
   * Computes err @ documentsRetrieved
   *  
   * NOTE: 
   * 
   */
  private double computeERR(double[] scores) {
    double score = 0.0;
    double decay = 1.0;

    for (int i = 0; i < Math.min(scores.length, this.documentsRetrieved); i++) {
      double r = (Math.pow(2, scores[i]) - 1) / Math.pow(2,maxJudgment);
      score += r * decay / (i+1);
      decay *= (1 - r);
    }
    return score;
  }
}
