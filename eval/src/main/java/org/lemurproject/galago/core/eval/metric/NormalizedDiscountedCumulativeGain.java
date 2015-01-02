/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.EvalDoc;
import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

import java.util.Arrays;

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
    for (EvalDoc doc : resultList.getIterator()) {
      // document judgments must be positive
      documentJudgments[index] = 0;
      if (judgments.get(doc.getName()) > 0) {
        documentJudgments[index] = judgments.get(doc.getName());
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
    for (int judgment : judgments.values()) {
      if (judgment > 0) {
        idealJudgments[index] = judgment;
        index++;
      }
    }

    Arrays.sort(idealJudgments);
    idealJudgments = reverse(idealJudgments);
    double normalizer = computeDCG(idealJudgments);

    if(normalizer != 0){
      return dcg / normalizer;
    }
    
    // if there are no relevant documents, 
    // the average is artificially defined as zero, to mimic trec_eval
    // Really, the output is NaN, or the query should be ignored.
    return 0.0;
  }

  /**
   * Reverse an array
   * @param input an array of doubles
   * @return a copy of the input array, reversed
   */
  private double[] reverse(double[] input) {
    double[] output = new double[input.length];
    for(int i=0; i<input.length; i++) {
      output[input.length-i-1] = input[i];
    }
    return output;
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
