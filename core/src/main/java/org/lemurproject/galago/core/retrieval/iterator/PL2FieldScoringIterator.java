// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.PL2FieldScorer;
import org.lemurproject.galago.tupleflow.Parameters;

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
public class PL2FieldScoringIterator extends ScoringFunctionIterator 
  implements DeltaScoringIterator {

  String partName;
  double max;
  double min = 0.0001;
  public int parentIdx = -1;
  public double weight;
  double beta;
  double log2;

  public PL2FieldScoringIterator(Parameters globalParams, NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(it, new PL2FieldScorer(globalParams, p, it));
    partName = p.getString("lengths");
    log2 = Math.log(2);
    if (it instanceof PositionIndexReader.TermCountIterator) {
      PositionIndexReader.TermCountIterator maxIter = (PositionIndexReader.TermCountIterator) it;
      max = function.score(maxIter.maximumCount(), maxIter.maximumCount());
    } else {
      max = 0;  // Means we have a null extent iterator
    }
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

    DeltaScoringContext ctx = (DeltaScoringContext)context;
    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double score = function.score(count, context.getLength(partName));
    score = (score > 0.0) ? score : min; // MY smoothing again
    double phi = ctx.potentials[parentIdx];
    double psi = phi + (weight * (score - max));
    double logpsi = Math.log(psi) / log2;
    double logphi = Math.log(phi) / log2;
    
    double t1 = beta * (phi - psi);
    double t2 = logpsi * ((phi*psi) + (0.5 * phi) + psi + 0.5);
    double t3 = logphi * ((phi*psi) + (0.5 * psi) + phi + 0.5);
    double den = (phi + 1) * (psi + 1);
    double diff = (t1 + t2 - t3) / den;

    ctx.runningScore += diff;
    ctx.potentials[parentIdx] = psi;
  }

  public void maximumDifference() {
    DeltaScoringContext ctx = (DeltaScoringContext)context;
    double phi = ctx.potentials[parentIdx];
    double psi = phi + (weight * (min - max));
    double logpsi = Math.log(psi) / log2;
    double logphi = Math.log(phi) / log2;

    double t1 = beta * (psi - phi);
    double t2 = logphi * ((phi*psi) + (0.5 * psi) + phi + 0.5);
    double t3 = logpsi * ((phi*psi) + (0.5 * phi) + psi + 0.5);
    double den = (phi + 1) * (psi + 1);
    double diff = (t1 + t2 - t3) / den;

    ctx.runningScore += diff;
    ctx.potentials[parentIdx] = psi;
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
