package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.EvalDoc;
import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;
import org.lemurproject.galago.core.eval.SimpleEvalDoc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This metric calculates AP with unjudged documents removed from the ranking.
 * This is when you want to make the point that
 * <strong>if</strong> you did more judgments, your retrieval model <strong>might</strong> be better.
 *
 * @author jfoley
 */
public class JudgedAveragePrecision extends QueryEvaluator {
  public JudgedAveragePrecision(String metric) {
    super(metric);
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    double sumPrecision = 0.0;
    int relevantCount = 0;

    List<SimpleEvalDoc> onlyJudged = new ArrayList<>();
    for (EvalDoc evalDoc : resultList.getIterator()) {
      if(judgments.isJudged(evalDoc.getName())) {
        onlyJudged.add(new SimpleEvalDoc(evalDoc));
      }
    }

    // Just in case it's not sorted:
    Collections.sort(onlyJudged, SimpleEvalDoc.byAscendingRank);

    for (int i = 0; i < onlyJudged.size(); i++) {
      EvalDoc doc = onlyJudged.get(i);
      int rank = i+1;
      if (judgments.isRelevant(doc.getName())) {
        relevantCount++;
        sumPrecision += relevantCount / (double) rank;
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

