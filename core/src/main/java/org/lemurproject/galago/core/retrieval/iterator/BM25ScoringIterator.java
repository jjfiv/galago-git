// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.EarlyTerminationScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.BM25Scorer;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "nodeDocumentCount", "maximumCount"})
@RequiredParameters(parameters = {"b", "k"})
public class BM25ScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  double weight;
  int parentIdx;
  double min;

  public BM25ScoringIterator(NodeParameters p, MovableLengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);
    this.setScoringFunction(new BM25Scorer(p, it));
    weight = p.get("w", 1.0);
    parentIdx = (int) p.get("pIdx", 0);
    max = getMaxTF(p, it);
    min = function.score(0, it.maximumCount());
  }

  /**
   * Minimized by having no occurrences.
   *
   * @return
   */
  public double minimumScore() {
    return min;
  }

  public double getWeight() {
    return weight;
  }

  @Override
  public void deltaScore(int count, int length) {
    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;
    double diff = weight * (function.score(count, length) - max);
    ctx.runningScore += diff;
  }

  @Override
  public void deltaScore(int length) {
    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }
    double diff = weight * (function.score(count, length) - max);
    ctx.runningScore += diff;
  }

  @Override
  public void deltaScore() {
    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }
    double diff = weight * (function.score(count, context.getLength()) - max);
    ctx.runningScore += diff;
  }

  @Override
  public void maximumDifference() {
    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;
    double diff = weight * (min - max);
    ctx.runningScore += diff;
    ////CallTable.increment("aux_flops");
  }

  @Override
  public void aggregatePotentials(EarlyTerminationScoringContext ctx) {
    // Nothing to do here
  }

  @Override
  public void setContext(ScoringContext ctx) {
    super.setContext(ctx);
    if (EarlyTerminationScoringContext.class.isAssignableFrom(ctx.getClass())) {
      EarlyTerminationScoringContext dctx = (EarlyTerminationScoringContext) ctx;
      if (dctx.members.contains(this)) return;     
      dctx.scorers.add(this);
      dctx.members.add(this);
      dctx.startingPotential += (max * weight);

    }
  }
}
