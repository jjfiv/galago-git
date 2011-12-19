/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;
import org.lemurproject.galago.core.retrieval.ScoredDocument;

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
    List<ScoredDocument> relevantRetrieved = getRelevantRetrieved(resultList, judgments);

    int i = 0;
    int j = 0;
    // 2006 bug fix...
    int nonRelevantCount = Math.min(totalRelevant, judgments.getNonRelevantJudgmentCount());
    List<ScoredDocument> judgedIrrelevantRetrieved = getIrrelevantRetrieved(resultList, judgments);
    List<ScoredDocument> irrelevant = judgedIrrelevantRetrieved.subList(0, Math.min(totalRelevant, judgedIrrelevantRetrieved.size()));

    double sum = 0;
    // if no negative judgments, num_rel_ret/num_rel (trec_eval 8).
    if (irrelevant.size() == 0) {
      sum = relevantRetrieved.size();
    }

    while (i < relevantRetrieved.size() && j < irrelevant.size()) {
      ScoredDocument rel = relevantRetrieved.get(i);
      ScoredDocument irr = irrelevant.get(j);

      if (rel.rank < irr.rank) {
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

  private List<ScoredDocument> getRelevantRetrieved(QueryResults resultList, QueryJudgments judgments) {
    ArrayList<ScoredDocument> relevantRetrieved = new ArrayList();
    for (ScoredDocument doc : resultList.getIterator()) {
      if (judgments.isRelevant(doc.documentName)) {
        relevantRetrieved.add(doc);
      }
    }
    return relevantRetrieved;
  }

  private List<ScoredDocument> getIrrelevantRetrieved(QueryResults resultList, QueryJudgments judgments) {
    ArrayList<ScoredDocument> irrelevantRetrieved = new ArrayList();
    for (ScoredDocument doc : resultList.getIterator()) {
      if (judgments.isNonRelevant(doc.documentName)) {
        irrelevantRetrieved.add(doc);
      }
    }
    return irrelevantRetrieved;
  }
}
