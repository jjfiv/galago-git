/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.FieldDeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeFrequency", "documentCount"})
public class DFRScoringIterator extends TransformIterator implements MovableScoreIterator {

  double lambda;
  double qfratio;
  double loge;
  double log2;
  MovableScoreIterator scorer;
  Parameters globals;

  public DFRScoringIterator(Parameters globalParams,
          NodeParameters parameters, MovableScoreIterator iterator) throws IOException {
    super(iterator);
    scorer = iterator;

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
    super.setContext(ctx);

    if (ctx instanceof FieldDeltaScoringContext) {
      FieldDeltaScoringContext pctx = (FieldDeltaScoringContext) ctx;

      // HAHAHAHA I copied this nasty hack!
      if (pctx.startingPotentials == null) {
        pctx.startingPotentials = new double[(int) globals.getLong("numberOfTerms")];
        pctx.potentials = new double[pctx.startingPotentials.length];
      }
      pctx.startingPotentials[pctx.quorumIndex] = scorer.maximumScore();
      pctx.startingPotential += transform(pctx.startingPotentials[pctx.quorumIndex]);
      for (int i = pctx.scorers.size() - 1; i >= 0; i--) {
        PL2FieldScoringIterator pfsi = (PL2FieldScoringIterator) pctx.scorers.get(i);
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
