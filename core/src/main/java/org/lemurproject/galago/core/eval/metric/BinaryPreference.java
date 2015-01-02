/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.EvalDoc;
import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>The binary preference measure, as presented in Buckley, Voorhees
 * "Retrieval Evaluation with Incomplete Information", SIGIR 2004.
 * This implemenation is the 'pure' version, which is the one
 * used in Buckley's trec_eval.</p>
 *
 * <p>The formula is:
 * <tt>1/R \sum_{r} 1 - |n ranked greater than r| / R</tt>
 * where R is the number of relevant documents for this topic, and
 * n is a member of the set of first R judged irrelevant documents
 * retrieved.</p>
 * 
 * @author trevor, sjh
 */
public class BinaryPreference extends QueryEvaluator {

  public BinaryPreference(String metric) {
    super(metric);
  }

  @Override
  public double evaluate(QueryResults resultList, QueryJudgments judgments) {
    // Edge case: no relevant documents retrieved
    // Edge case: more relevant documents than count retrieved

    int totalRelevant = judgments.getRelevantJudgmentCount();
    if (totalRelevant == 0) {
      return 0;
    }
    List<EvalDoc> relevantRetrieved = getRelevantRetrieved(resultList, judgments);

    int i = 0;
    int j = 0;
    // 2006 bug fix...
    int nonRelevantCount = Math.min(totalRelevant, judgments.getNonRelevantJudgmentCount());
    List<EvalDoc> judgedIrrelevantRetrieved = getIrrelevantRetrieved(resultList, judgments);
    List<EvalDoc> irrelevant = judgedIrrelevantRetrieved.subList(0, Math.min(totalRelevant, judgedIrrelevantRetrieved.size()));

    double sum = 0;
    // if no negative judgments, num_rel_ret/num_rel (trec_eval 8).
    if (irrelevant.size() == 0) {
      sum = relevantRetrieved.size();
    }

    while (i < relevantRetrieved.size() && j < irrelevant.size()) {
      EvalDoc rel = relevantRetrieved.get(i);
      EvalDoc irr = irrelevant.get(j);

      if (rel.getRank() < irr.getRank()) {
        // we've just seen a relevant document;
        // how many of the irrelevant set are ahead of us?
        assert j <= totalRelevant;
        sum += 1.0 - ((double) j / (double) nonRelevantCount);
        i++;
      } else {
        j++;
      }
    }

    return sum / (double) totalRelevant;
  }

  private List<EvalDoc> getRelevantRetrieved(QueryResults resultList, QueryJudgments judgments) {
    ArrayList<EvalDoc> relevantRetrieved = new ArrayList<>();
    for (EvalDoc doc : resultList.getIterator()) {
      if (judgments.isRelevant(doc.getName())) {
        relevantRetrieved.add(doc);
      }
    }
    return relevantRetrieved;
  }

  private List<EvalDoc> getIrrelevantRetrieved(QueryResults resultList, QueryJudgments judgments) {
    ArrayList<EvalDoc> irrelevantRetrieved = new ArrayList<>();
    for (EvalDoc doc : resultList.getIterator()) {
      if (judgments.isNonRelevant(doc.getName())) {
        irrelevantRetrieved.add(doc);
      }
    }
    return irrelevantRetrieved;
  }
}
