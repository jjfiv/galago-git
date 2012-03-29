/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeFrequency", "documentCount"})
public class DFRScoringIterator extends TransformIterator implements MovableScoreIterator {

  double lambda;
  double qfratio;
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
    globals = globalParams;
  }

  private double transform(double ts) {
    double f1 = ts * Math.log(ts / lambda) / Utility.log2;
    double f2 = (lambda - ts) * Utility.loge;
    double f3 = 0.5 * Math.log(2 * Math.PI * ts) / Utility.log2;
    double risk = 1.0 / (ts + 1.0);
    return qfratio * risk * (f1 + f2 + f3);
  }

  public void setContext(ScoringContext ctx) {
    super.setContext(ctx);

    if (ctx instanceof DeltaScoringContext) {
      DeltaScoringContext dctx = (DeltaScoringContext) ctx;

      // Need to do this at the aggregate level     
      dctx.startingPotentials[dctx.quorumIndex] = scorer.maximumScore();
      /*
      System.err.printf("at qidx=%d: startingP=%f, inc=%f\n", dctx.quorumIndex,
			dctx.startingPotentials[dctx.quorumIndex], transform(dctx.startingPotentials[dctx.quorumIndex]));
      */
      dctx.startingPotential += transform(dctx.startingPotentials[dctx.quorumIndex]);
      dctx.quorumIndex++;
    }
  }

  @Override
  public double score() {
    double tscore = scorer.score();
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
