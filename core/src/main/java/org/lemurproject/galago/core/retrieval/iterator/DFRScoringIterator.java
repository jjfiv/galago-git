/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.galagosearch.core.retrieval.query.NodeParameters;
import org.galagosearch.core.retrieval.structured.PL2FContext;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.retrieval.structured.ScoringContext;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeFrequency", "documentCount"})
public class DFRScoringIterator extends TransformIterator {

  double lambda;
  double qfratio;
  double loge;
  double log2;
  ScoreValueIterator scorer;
  Parameters globals;

  public DFRScoringIterator(Parameters globalParams,
          NodeParameters parameters, ScoreValueIterator iterator) throws IOException {
    super(iterator);
    scorer = (ScoreValueIterator) iterator;

    // Set the qf ratio
    int qfmax = (int) parameters.get("qfmax", 1);
    int qf = (int) parameters.get("qf", 1);
    qfratio = (qf + 0.0) / (qfmax + 0.0);

    // Set the lambda
    long termFrequency = parameters.getLong("nodeFrequency");
    long documentCount = parameters.getLong("documentCount");
    lambda = (termFrequency + 0.0) / (documentCount + 0.0);
    log2 = Math.log(2);
    loge = Math.log(Math.E) / log2;
    globals = globalParams;
  }

  private double transform(double ts) {
    double f1 = ts * Math.log(ts / lambda) / log2;
    double f2 = (lambda - ts) * loge;
    double f3 = 0.5 * Math.log(2 * Math.PI * ts) / log2;
    double risk = 1.0 / (ts + 1.0);
    return qfratio * risk * (f1 + f2 + f3);
  }

  public void setContext(ScoringContext ctx) {
    if (context != null) {
      return;
    }
    super.setContext(ctx);

    if (ctx instanceof PL2FContext) {
      PL2FContext pctx = (PL2FContext) ctx;

      // HAHAHAHA I copied this nasty hack!
      if (pctx.startingSubtotals == null) {
        pctx.startingSubtotals = new double[(int) globals.getLong("numberOfTerms")];
        pctx.subtotals = new double[pctx.startingSubtotals.length];
      }
      pctx.startingSubtotals[pctx.quorumIndex] = scorer.maximumScore();
      pctx.startingPotential += transform(pctx.startingSubtotals[pctx.quorumIndex]);
      //System.err.printf("(%d) starting subtotal: %f\n", pctx.quorumIndex, pctx.startingSubtotals[pctx.quorumIndex]);
      for (int i = pctx.scorers.size() - 1; i >= 0; i--) {
        PL2FieldScoringIterator pfsi = pctx.scorers.get(i);
        if (pfsi.parentIdx == -1) {
          pfsi.parentIdx = pctx.quorumIndex;
          pfsi.beta = Math.log(lambda) / log2 + (lambda * loge) + 
                  ((0.5 * (Math.log(2 * Math.PI)/log2)) + loge);
        } else {
          break;
        }
      }
      pctx.quorumIndex++;
    }
  }

  @Override
  public double score() {
    double tscore = scorer.score();
    //System.err.printf("doc %d, score: %f\n", context.document, tscore);
    double transformedScore = transform(tscore);
    return transformedScore;
  }

  @Override
  public double score(ScoringContext context) {
    double tscore = scorer.score(context);
    double transformedScore = transform(tscore);
    return transformedScore;
  }

  @Override
  public double maximumScore() {
    return transform(scorer.maximumScore());
  }

  @Override
  public double minimumScore() {
    return transform(scorer.minimumScore());
  }
}
