/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.galagosearch.core.retrieval.query.NodeParameters;
import org.galagosearch.core.retrieval.structured.PRMSContext;
import org.galagosearch.core.retrieval.structured.ScoringContext;
import org.galagosearch.tupleflow.Parameters;

/**
 * Not implemented to work over raw counts because it shouldn't blindly
 * be applied to counts - doing that can result in -Infinity scores everywhere.
 * Therefore, this applies to scores, meaning to you had to make a conscious decision
 * to pass a raw count up, and if things go wrong, it's your fault.
 * 
 * @author irmarc
 */
public class LogarithmIterator extends TransformIterator {

  ScoreValueIterator scorer;
  Parameters globals;

  public LogarithmIterator(Parameters globalParams, NodeParameters params, ScoreValueIterator svi) {
    super(svi);
    scorer = svi;
    globals = globalParams; // <--- incoming filthy hack
    context = null;
  }

  @Override
  public double score() {
    return Math.log(scorer.score());
  }

  @Override
  public double score(ScoringContext context) {
    return Math.log(scorer.score(context));
  }

  @Override
  public double maximumScore() {
    return Math.log(scorer.maximumScore());
  }

  @Override
  public double minimumScore() {
    return Math.log(scorer.minimumScore());
  }

  public void setContext(ScoringContext ctx) {
    if (context != null) return;
    super.setContext(ctx);
    if (ctx instanceof PRMSContext) {
      PRMSContext pctx = (PRMSContext) ctx;

      // Jesus this is so gross...this is not what the quorum index is for
      // but it gets the job done
      if (pctx.startingSubtotals == null) {
        // this is possibly the smelliest crappy hack ever...
        pctx.startingSubtotals = new double[(int) globals.getLong("numberOfTerms")];
        pctx.subtotals = new double[pctx.startingSubtotals.length];
      }
      pctx.startingSubtotals[pctx.quorumIndex] =
              scorer.maximumScore();
      //System.err.printf("(%d) starting subtotal: %f\n", pctx.quorumIndex, pctx.startingSubtotals[pctx.quorumIndex]);
      for (int i = pctx.scorers.size() - 1; i >= 0; i--) {
        DirichletProbabilityScoringIterator dpsi =
                pctx.scorers.get(i);
        if (dpsi.parentIdx == -1) {
          dpsi.parentIdx = pctx.quorumIndex;
        } else {
          break;
        }
      }
      pctx.quorumIndex++;
    }
  }
}
