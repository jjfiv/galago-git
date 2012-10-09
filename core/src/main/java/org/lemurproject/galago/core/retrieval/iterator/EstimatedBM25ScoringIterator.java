/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.EstimatedDocument;
import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.processing.SoftDeltaScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.BM25Scorer;
import org.lemurproject.galago.core.scoring.Estimator;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount"})
@RequiredParameters(parameters = {"w", "syntheticCounts"})
public class EstimatedBM25ScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator, Estimator {

  double weight;
  double min;
  double documentCount;
  MinimumCountConjunctionIterator mcci;
  double[] range;
  int maxcount;
  public double lowEstimate, hiEstimate;
  boolean storedSyntheticCounts;

  public EstimatedBM25ScoringIterator(NodeParameters p, MinimumCountConjunctionIterator it)
          throws IOException {
    super(p, it); // have to fake it at first
    mcci = it;
    range = new double[2];
    weight = p.getDouble("w");
    documentCount = 0.0 + p.getLong("documentCount");
    p.set("nodeDocumentCount", 1);
    this.setScoringFunction(new BM25Scorer(p, it));

    // (4) -- see below in setContext
    if (p.containsKey("maximumCount")) {
      maxcount = (int) p.getLong("maximumCount");
    } else if (it != null) {
      maxcount = it.maximumCount();
    }
    storedSyntheticCounts = p.containsKey("syntheticCounts");
  }

  @Override
  public double getWeight() {
    return weight;
  }

  @Override
  public void maximumDifference() {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    double diff = weight * (min - max);
    ctx.runningScore += diff;
  }

  /**
   * Some extra work to do here. We need to do (in this order) 1) Add this as a
   * scorer 2) Set the low estimate for df (1) 3) Set the high df estimate (min
   * of all component nodes) 4) Get the maxcount (don't use maxTF as it produces
   * a score) 5) Use the maxcount and the low estimate to produce the max
   * potential 6) Set min = 0 (artifact of this particular scoring function) 7)
   * Add the maximum to the potential
   */
  @Override
  public void setContext(ScoringContext ctx) {
    super.setContext(ctx);
    if (SoftDeltaScoringContext.class.isAssignableFrom(ctx.getClass())) {
      SoftDeltaScoringContext dctx = (SoftDeltaScoringContext) ctx;
      // (1)
      dctx.scorers.add(this);
      if (!dctx.hi_accumulators.containsKey(this)) {
        // (2)
        hiEstimate = Math.log(documentCount / 1.5);

        // (3)
        int estimate = mcci.getBestDocumentFrequency();
        estimate = (estimate == Integer.MAX_VALUE) ? (int) documentCount : estimate;
        lowEstimate = Math.log(documentCount / (estimate + 0.5));
      } else {
        hiEstimate = lowEstimate = Math.log(documentCount / (dctx.hi_accumulators.get(this) + 0.5));
      }

      // (4) -- above
      // (5)
      max = (maxcount == 0) ? 0 : ((BM25Scorer) function).score(maxcount, maxcount, hiEstimate);
      // (6)
      min = 0;
      // (7)
      dctx.startingPotential += (max * weight);

    }
  }

  @Override
  public void aggregatePotentials(DeltaScoringContext ctx) {
    // Nothing to do here
  }

  @Override
  public void deltaScore() {
    throw new UnsupportedOperationException("This particular node does not delta score directly");
  }

  /**
   * We use the low estimate of the df for the high estimate here, b/c low df
   * --> high idf --> more importance --> higher score.
   *
   * @param context
   * @return
   */
  @Override
  public double[] estimate(SoftDeltaScoringContext context) {
    int count = 0;

    if (iterator.hasMatch(context.document)) {
      count = mcci.count();
    }

    if (count == 0) {
      range[0] = range[1] = 0;
    } else {
      double score = ((BM25Scorer) function).score(count, context.getLength(), hiEstimate);
      range[1] = weight * score;

      score = ((BM25Scorer) function).score(0, context.getLength(), lowEstimate);
      range[0] = weight * score;
    }
    return range;
  }

  @Override
  public void adjustEstimate(SoftDeltaScoringContext context, EstimatedDocument document, int idx) {
    if (idx == 0) {
      document.min = 0;
    }
    double idf = Math.log(documentCount / context.hi_accumulators.get(this) + 0.5);
    double score = ((BM25Scorer) function).score(0, document.length, idf);
    document.min += weight * score;

  }

  @Override
  public double[] estimateWithUpdate(SoftDeltaScoringContext context, int idx) {
    int count = 0;

    if (iterator.hasMatch(context.document)) {
      count = mcci.count();
    }

    int diff = mcci.getDFDifference(context.document);
    if (diff > 0) {
      context.hi_accumulators.adjustValue(this, -diff);
    }
    context.counts[idx] = (short) count;
    double idf = Math.log(documentCount / context.lo_accumulators.get(this) + 0.5);
    double score = ((BM25Scorer) function).score(count, context.getLength(), idf);
    range[1] = weight * score;

    idf = Math.log(documentCount / context.hi_accumulators.get(this) + 0.5);
    score = ((BM25Scorer) function).score(0, context.getLength(), idf);
    range[0] = weight * score;

    return range;
  }

  @Override
  public void deltaScore(int count, int length) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void deltaScore(int length) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
