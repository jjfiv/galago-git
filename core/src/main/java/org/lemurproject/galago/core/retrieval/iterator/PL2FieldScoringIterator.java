/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.galagosearch.core.index.disk.PositionIndexReader;
import org.galagosearch.core.retrieval.query.NodeParameters;
import org.galagosearch.core.retrieval.structured.FieldScoringContext;
import org.galagosearch.core.retrieval.structured.PL2FContext;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.scoring.PL2FieldScorer;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * A ScoringIterator that makes use of the PL2F scoring function
 * for converting a document field count into a score.
 *
 * In the delta-function form, computes the incremental change when this
 * nodes potential is replaced by the concrete count from a document.
 * 
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount"})
public class PL2FieldScoringIterator extends ScoringFunctionIterator {

  String partName;
  double max;
  double min = 0.0001;
  public int parentIdx = -1;
  public double weight;
  double beta;
  double log2;

  public PL2FieldScoringIterator(Parameters globalParams, NodeParameters p, CountValueIterator it)
          throws IOException {
    super(it, new PL2FieldScorer(globalParams, p, it));
    partName = p.getString("lengths");
    log2 = Math.log(2);
    if (it instanceof PositionIndexReader.TermCountIterator) {
      PositionIndexReader.TermCountIterator maxIter = (PositionIndexReader.TermCountIterator) it;
      max = function.score(maxIter.maximumCount(), maxIter.maximumCount());
      //System.err.printf("%s max: %f\n", this.toString(), max);
    } else {
      max = 0;  // Means we have a null extent iterator
    }
  }

  @Override
  public double score() {
    if (context instanceof FieldScoringContext) {
      int count = 0;

      if (iterator.currentCandidate() == context.document) {
        count = ((CountIterator) iterator).count();
      }
      double score = function.score(count, ((FieldScoringContext) context).lengths.get(partName));
      score = (score > 0.0) ? score : min; // MY smoothing.
      return score;
    } else {
      return super.score();
    }
  }

  public void score(PL2FContext ctx) {
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double score = function.score(count, ctx.lengths.get(partName));
    score = (score > 0.0) ? score : min; // MY smoothing again
    double phi = ctx.subtotals[parentIdx];
    double psi = phi + (weight * (score - max));
    double logpsi = Math.log(psi) / log2;
    double logphi = Math.log(phi) / log2;
    //System.err.printf("idx=%d, beta=%f, phi=%f, psi=%f\n", parentIdx, beta, phi, psi);
    
    double t1 = beta * (phi - psi);
    double t2 = logpsi * ((phi*psi) + (0.5 * phi) + psi + 0.5);
    double t3 = logphi * ((phi*psi) + (0.5 * psi) + phi + 0.5);
    double den = (phi + 1) * (psi + 1);
    double diff = (t1 + t2 - t3) / den;
    /*
    System.err.printf("%s score: (t1=%f, t2=%f, t3=%f, den=%f, diff=%f\n",
            this.toString(), t1, t2, t3, den, diff);
    System.err.printf("running: %f -> %f, subt: %f -> %f\n", ctx.runningScore,
            ctx.runningScore+diff, ctx.subtotals[parentIdx], psi);*/
    ctx.runningScore += diff;
    ctx.subtotals[parentIdx] = psi;
  }

  public void maximumAdjustment(PL2FContext ctx) {
    double phi = ctx.subtotals[parentIdx];
    double psi = phi + (weight * (min - max));
    double logpsi = Math.log(psi) / log2;
    double logphi = Math.log(phi) / log2;

    double t1 = beta * (psi - phi);
    double t2 = logphi * ((phi*psi) + (0.5 * psi) + phi + 0.5);
    double t3 = logpsi * ((phi*psi) + (0.5 * phi) + psi + 0.5);
    double den = (phi + 1) * (psi + 1);
    double diff = (t1 + t2 - t3) / den;
    /*
    System.err.printf("%s maxAdjust: (t1=%f, t2=%f, t3=%f, den=%f, diff=%f\n",
            this.toString(), t1, t2, t3, den, diff);
     */
    ctx.runningScore += diff;
    ctx.subtotals[parentIdx] = psi;
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
