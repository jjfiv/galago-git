/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Not implemented to work over raw counts because it shouldn't blindly
 * be applied to counts - doing that can result in -Infinity scores everywhere.
 * Therefore, this applies to scores, meaning to you had to make a conscious decision
 * to pass a raw count up, and if things go wrong, it's your fault.
 * 
 * @author irmarc
 */
public class LogarithmIterator extends TransformIterator implements MovableScoreIterator {

  MovableScoreIterator scorer;
  Parameters globals;

  public LogarithmIterator(Parameters globalParams, NodeParameters params, MovableScoreIterator svi) {
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
    if (DeltaScoringContext.class.isAssignableFrom(ctx.getClass())) {
      DeltaScoringContext dctx = (DeltaScoringContext) ctx;

      // Jesus this is so gross...this is not what the quorum index is for
      // but it gets the job done
      if (dctx.startingPotentials == null) {
        // this is possibly the smelliest crappy hack ever...
        dctx.startingPotentials = new double[(int) globals.getLong("numberOfTerms")];
        dctx.potentials = new double[dctx.startingPotentials.length];
      }
      dctx.startingPotentials[dctx.quorumIndex] = scorer.maximumScore();
      for (int i = dctx.scorers.size() - 1; i >= 0; i--) {
        DirichletProbabilityScoringIterator dpsi = 
                (DirichletProbabilityScoringIterator) dctx.scorers.get(i);
        if (dpsi.parentIdx == -1) {
          dpsi.parentIdx = dctx.quorumIndex;
        } else {
          break;
        }
      }
      dctx.quorumIndex++;
    }
  }
}
