// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.EstimatedDocument;
import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.processing.SoftDeltaScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.scoring.DirichletScorer;
import org.lemurproject.galago.core.scoring.Estimator;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"maximumCount","collectionLength", "documentCount"})
@RequiredParameters(parameters = {"w", "collapsing, syntheticCounts"})
public class EstimatedDirichletScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator, Estimator {

  double weight;
  double min;
  long collectionLength, documentCount;
  MinimumCountConjunctionIterator mcci;
  double[] range;
  boolean collapsing;
  public double lowEstimate, hiEstimate;
  boolean storedSyntheticCounts;
  int maxcount = 0;

  public EstimatedDirichletScoringIterator(NodeParameters p, MovableLengthsIterator ls, MinimumCountConjunctionIterator it)
          throws IOException {
    super(p, ls, it); // have to fake it at first
    mcci = it;
    range = new double[2];
    collapsing = p.get("collapsing", true);
    weight = p.getDouble("w");
    collectionLength = p.getLong("collectionLength");

    // now create/set the function - the prob won't matter. We ignore it.
    p.set("nodeFrequency", 1);
    this.setScoringFunction(new DirichletScorer(p, it));

    documentCount = p.getLong("documentCount");

    // (4)
    if (p.containsKey("maximumCount")) {
      maxcount = (int) p.getLong("maximumCount");
    } else if (it != null) {
      maxcount = it.maximumCount();
    }

    storedSyntheticCounts = p.containsKey("syntheticCounts");
  }

  /**
   * The way the count iterator works will take care of the "estimating" part
   * for us here.
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

    double cf, score;
    //cf = (context.lo_accumulators.get(this) + 0.5) / this.collectionLength;
    score = ((DirichletScorer) function).score(count, context.getLength(), hiEstimate);
    range[1] = weight * score;

    if (count == 0) {
      range[0] = range[1];
    } else {
      //cf = (context.lo_accumulators.get(this) + 0.0) / this.collectionLength;
      score = ((DirichletScorer) function).score(0, context.getLength(), lowEstimate);
      range[0] = weight * score;
    }
    return range;
  }

  @Override
  public void adjustEstimate(SoftDeltaScoringContext context, EstimatedDocument document, int idx) {
    if (idx == 0) {
      document.max = 0.0;
    }
    double cf = (context.hi_accumulators.get(this) + 0.0) / this.collectionLength;
    double score = ((DirichletScorer) function).score(document.counts[idx], context.getLength(), cf);
    document.max += weight * score;
  }

  @Override
  public double[] estimateWithUpdate(SoftDeltaScoringContext context, int idx) {
    int count = 0;

    if (iterator.hasMatch(context.document)) {
      count = mcci.count();
    }

    int diff = mcci.getCFDifference(context.document);
    if (diff > 0) {
      //context.hi_accumulators.adjustValue(this, -diff);
      hiEstimate -= diff;
    }

    context.counts[idx] = (short) count;
    double cf, score;
    if (count == 0 && collapsing) {
      //cf = (context.lo_accumulators.get(this) + 1.0) / this.collectionLength;
      cf = (lowEstimate + 0.5) / this.collectionLength;
      score = ((DirichletScorer) function).score(count, context.getLength(), cf);
      range[1] = weight * score;
    } else {
      //cf = (context.hi_accumulators.get(this) + 0.0) / this.collectionLength;
      cf = hiEstimate / this.collectionLength;
      score = ((DirichletScorer) function).score(count, context.getLength(), cf);
      range[1] = weight * score;
    }

    //cf = (context.lo_accumulators.get(this) + 0.0) / this.collectionLength;
    cf = lowEstimate / this.collectionLength;
    score = ((DirichletScorer) function).score(0, context.getLength(), cf);
    range[0] = weight * score;

    return range;
  }

  @Override
  public void deltaScore() {
    throw new UnsupportedOperationException("This particular node does not delta score directly");
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

  @Override
  public void setContext(ScoringContext ctx) {
    super.setContext(ctx);
    if (SoftDeltaScoringContext.class.isAssignableFrom(ctx.getClass())) {
      SoftDeltaScoringContext dctx = (SoftDeltaScoringContext) ctx;
      dctx.scorers.add(this);
      dctx.startingPotential += (max * weight);
      if (!dctx.hi_accumulators.containsKey(this)) {
        lowEstimate = 1.0 / this.collectionLength;

        if (collapsing) {
          hiEstimate = (1.5) / this.collectionLength;
        } else {
          int estimate = mcci.getBestCollectionFrequency();
          estimate = (estimate == Integer.MAX_VALUE) ? (int) collectionLength : estimate;
          hiEstimate = (estimate + 0.0) / this.collectionLength;
        }
      } else {
        lowEstimate = hiEstimate = (dctx.hi_accumulators.get(this) + 0.0) / this.collectionLength;
      }
      // Increase doc size by 250%
      int avgDocLength = (int) Math.round(2.5 * (collectionLength + 0.0) / (documentCount + 0.0));

      // Allows for a slightly more conservative "worst-case"
      min = ((DirichletScorer) function).score(0, avgDocLength, lowEstimate);
      max = ((DirichletScorer) function).score(maxcount, avgDocLength, hiEstimate);
    }
  }

  @Override
  public void aggregatePotentials(DeltaScoringContext ctx) {
    // Doesn't do anything
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
