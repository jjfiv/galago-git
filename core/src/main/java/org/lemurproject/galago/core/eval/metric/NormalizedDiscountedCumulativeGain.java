/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import java.util.Arrays;
import java.util.TreeMap;
import org.apache.commons.lang.ArrayUtils;
import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;
import org.lemurproject.galago.core.retrieval.ScoredDocument;

/** 
 * <p>Normalized Discounted Cumulative Gain </p>
 *
 * This measure was introduced in Jarvelin, Kekalainen, "IR Evaluation Methods
 * for Retrieving Highly Relevant Documents" SIGIR 2001.  I copied the formula
 * from Vassilvitskii, "Using Web-Graph Distance for Relevance Feedback in Web
 * Search", SIGIR 2006.
 *
 * Score = N \sum_i (2^{r(i)} - 1) / \log(1 + i)
 *
 * Where N is such that the score cannot be greater than 1.  We compute this
 * by computing the DCG (unnormalized) of a perfect ranking.
 *
 * @author trevor, sjh
 */
public class NormalizedDiscountedCumulativeGain extends QueryEvaluator {

  int documentsRetrieved;

  public NormalizedDiscountedCumulativeGain(String metric, int documentsRetrieved) {
    super(metric);
    this.documentsRetrieved = documentsRetrieved;
  }

  public NormalizedDiscountedCumulativeGain(String metric) {
    super(metric);
    documentsRetrieved = Integer.MAX_VALUE;
  }

  public NormalizedDiscountedCumulativeGain(int documentsRetrieved) {
    super("ndcg" + documentsRetrieved);
    this.documentsRetrieved = documentsRetrieved;
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    // compute dcg:
    double[] documentJudgments = new double[resultList.size()];
    int index = 0;
    for (ScoredDocument doc : resultList.getIterator()) {
      // document judgments must be positive
      documentJudgments[index] = 0;
      if (judgments.get(doc.documentName) > 0) {
        documentJudgments[index] = judgments.get(doc.documentName);
      }
      index++;
    }

    double dcg = computeDCG(documentJudgments);
    
    // the normalizer represents the highest possible DCG score
    // that could possibly be attained.  we compute this by taking the relevance
    // judgments, ordering them by relevance value (highly relevant documents first)
    // then calling that the ranked list, and computing its DCG value.
    double[] idealJudgments = new double[judgments.size()];
    index = 0;
    for (int judgment : judgments.getIterator()) {
      if (judgment > 0) {
        idealJudgments[index] = judgment;
        index++;
      }
    }

    Arrays.sort(idealJudgments);
    ArrayUtils.reverse(idealJudgments);
    double normalizer = computeDCG(idealJudgments);

    return dcg / normalizer;
  }

  /**
   * Computes dcg @ documentsRetrieved
   *  
   */
  private double computeDCG(double[] gains) {
    double dcg = 0.0;
    for (int i = 0; i < Math.min(gains.length, this.documentsRetrieved); i++) {
      dcg += (Math.pow(2, gains[i]) - 1.0) / Math.log(i + 2);
    }
    return dcg;
  }
}
