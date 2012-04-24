// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.PL2FieldScorer;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * A ScoringIterator that makes use of the PL2F scoring function for converting
 * a document field count into a score.
 *
 * In the delta-function form, computes the incremental change when this nodes
 * potential is replaced by the concrete count from a document.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount"})
@RequiredParameters(parameters = {"c"})
public class PL2FieldScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  String partName;
  double max;
  double min = 0.0001;
  int parentIdx = -1;
  double weight;
  double beta;

  public PL2FieldScoringIterator(NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(it, new PL2FieldScorer(p, it));
    partName = p.getString("lengths");
    weight = p.getDouble("w");
    parentIdx = (int) p.getLong("pIdx");
    long termFrequency = p.getLong("nf");
    long documentCount = p.getLong("dc");
    double lambda = (termFrequency + 0.0) / (documentCount + 0.0);
    beta = Math.log(lambda) / Utility.log2 + (lambda * Utility.loge)
            + ((0.5 * (Math.log(2 * Math.PI) / Utility.log2)) + Utility.loge);
    if (it instanceof PositionIndexReader.TermCountIterator) {
      PositionIndexReader.TermCountIterator maxIter = (PositionIndexReader.TermCountIterator) it;
      max = function.score(maxIter.maximumCount(), maxIter.maximumCount());
    } else {
      max = 0;  // Means we have a null extent iterator
    }
  }

  @Override
  public void setContext(ScoringContext ctx) {
    super.setContext(ctx);
    if (DeltaScoringContext.class.isAssignableFrom(ctx.getClass())) {
      DeltaScoringContext dctx = (DeltaScoringContext) ctx;
      dctx.scorers.add(this);
    }
  }

  @Override
  public ScoringContext getContext() {
    return this.context;
  }

  @Override
  public double score() {
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }
    double score = function.score(count, context.getLength(partName));
    score = (score > 0.0) ? score : min; // MY smoothing.
    return score;
  }

  public void deltaScore() {
    int count = 0;

    DeltaScoringContext ctx = (DeltaScoringContext) context;
    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double score = function.score(count, context.getLength(partName));
    score = (score > 0.0) ? score : min; // MY smoothing again
    double phi = ctx.potentials[parentIdx];
    double psi = phi + (weight * (score - max));
    double logpsi = Math.log(psi) / Utility.log2;
    double logphi = Math.log(phi) / Utility.log2;

    double t1 = beta * (phi - psi);
    double t2 = logpsi * ((phi * psi) + (0.5 * phi) + psi + 0.5);
    double t3 = logphi * ((phi * psi) + (0.5 * psi) + phi + 0.5);
    double den = (phi + 1) * (psi + 1);
    double diff = (t1 + t2 - t3) / den;
    ctx.runningScore += diff;

    /*
     * System.err.printf("idx=%d, beta=%f, score=%f, logpsi=%f, phi=%f, psi=%f,
     * weight=%f, max=%f, den=%f, running=%f\n", parentIdx, beta, score, logpsi,
     * phi, psi, weight, max, den, ctx.runningScore);
     */

    ctx.potentials[parentIdx] = psi;
  }

  public void maximumDifference() {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    double phi = ctx.potentials[parentIdx];
    double psi = phi + (weight * (min - max));
    double logpsi = Math.log(psi) / Utility.log2;
    double logphi = Math.log(phi) / Utility.log2;

    double t1 = beta * (psi - phi);
    double t2 = logphi * ((phi * psi) + (0.5 * psi) + phi + 0.5);
    double t3 = logpsi * ((phi * psi) + (0.5 * phi) + psi + 0.5);
    double den = (phi + 1) * (psi + 1);
    double diff = (t1 + t2 - t3) / den;

    ctx.runningScore += diff;
    ctx.potentials[parentIdx] = psi;
  }

  public void aggregatePotentials(DeltaScoringContext ctx) {
    // do nothing
  }

  @Override
  public double maximumScore() {
    return max;
  }

  @Override
  public double minimumScore() {
    return min;
  }
}
