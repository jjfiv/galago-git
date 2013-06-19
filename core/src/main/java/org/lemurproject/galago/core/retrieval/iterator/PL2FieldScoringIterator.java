// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.retrieval.processing.EarlyTerminationScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.PL2FieldScorer;
import org.lemurproject.galago.tupleflow.Parameters;
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
  double min = 0.0001;
  int parentIdx = -1;
  double weight;
  double beta;

  public PL2FieldScoringIterator(NodeParameters p, MovableLengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);
    this.setScoringFunction(new PL2FieldScorer(p, it));
    partName = p.getString("lengths");
    weight = p.getDouble("w");
    parentIdx = (int) p.getLong("pIdx");
    long termFrequency = p.getLong("nf");
    long documentCount = p.getLong("dc");
    double lambda = (termFrequency + 0.0) / (documentCount + 0.0);
    beta = Math.log(lambda) / Utility.log2 + (lambda * Utility.loge_base2)
            + ((0.5 * (Math.log(2 * Math.PI) / Utility.log2)) + Utility.loge_base2);
    max = getMaxTF(p, it);
  }

  @Override
  public void setContext(ScoringContext ctx) {
    super.setContext(ctx);
    if (EarlyTerminationScoringContext.class.isAssignableFrom(ctx.getClass())) {
      EarlyTerminationScoringContext dctx = (EarlyTerminationScoringContext) ctx;
      if (dctx.members.contains(this)) return;
      dctx.scorers.add(this);
      dctx.members.add(this);
    }
  }

  public double getWeight() {
    return weight;
  }

  @Override
  public double score() {
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }
    double score = function.score(count, this.lengthsIterator.getCurrentLength());
    score = (score > 0.0) ? score : min; // MY smoothing.
    return score;
  }

  @Override
  public void deltaScore(int count, int length) {

    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;

    double score = function.score(count, length);
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

    ctx.potentials[parentIdx] = psi;
  }

  @Override
  public void deltaScore(int length) {
    int count = 0;

    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;
    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double score = function.score(count, length);
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
    ctx.potentials[parentIdx] = psi;
  }

  @Override
  public void deltaScore() {
    int count = 0;

    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;
    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double score = function.score(count, lengthsIterator.getCurrentLength());
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

    ctx.potentials[parentIdx] = psi;
  }

  @Override
  public void maximumDifference() {
    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;
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

  @Override
  public void aggregatePotentials(EarlyTerminationScoringContext ctx) {
    // do nothing
  }

  @Override
  public double minimumScore() {
    return min;
  }
}
