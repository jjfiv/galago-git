package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.EvalDoc;
import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

/**
 * This metric calculates AP as if unjudged documents are relevant.
 * This is when you want to make the point that
 * <strong>if</strong> the stars align <strong>and</strong> your retrieval model is awesome,
 * all the unjudged documents in the top k are actually correct.
 *
 * @author jfoley
 */
public class UnjudgedAveragePrecision extends QueryEvaluator {
  private final int k;

  public UnjudgedAveragePrecision(String metric, int k) {
    super(metric);
    this.k = k;
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    double sumPrecision = 0.0;
    int relevantCount = 0;

    for (EvalDoc doc : resultList.getIterator()) {
      int rank = doc.getRank();
      boolean consideredRel = false;

      // Consider relevant if judged relevant or judgment missing while rank is high (rank <= k)
      if(judgments.containsKey(doc.getName())) {
        consideredRel = judgments.isRelevant(doc.getName());
      } else if (rank <= k) {
        consideredRel = true;
      }

      if (consideredRel) {
        relevantCount++;
        sumPrecision += relevantCount / (double) doc.getRank();
      }
    }

    if (judgments.getRelevantJudgmentCount() > 0) {
      return (double) sumPrecision / judgments.getRelevantJudgmentCount();
    }
    // if there are no relevant documents,
    // the average is artificially defined as zero, to mimic trec_eval
    // Really, the output is NaN, or the query should be ignored.
    return 0.0;
  }

  public static QueryEvaluator create(String lowerMetric) {
    String remaining = lowerMetric.replace("muap", "").trim();
    int k = 10;
    if(!remaining.isEmpty()) {
      k = Integer.parseInt(remaining);
    }
    return new UnjudgedAveragePrecision("muap", k);
  }
}
