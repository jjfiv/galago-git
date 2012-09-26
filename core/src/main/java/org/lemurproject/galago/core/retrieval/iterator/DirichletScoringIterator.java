// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.DirichletScorer;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * A ScoringIterator that makes use of the DirichletScorer function for
 * converting a count into a score.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "collectionProbability", "maximumCount"})
@RequiredParameters(parameters = {"mu"})
public class DirichletScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  double weight;
  int parentIdx;
  double min;

  public DirichletScoringIterator(NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(p, it, new DirichletScorer(p, it));
    weight = p.get("w", 1.0);
    parentIdx = (int) p.get("pIdx", 0);
    max = getMaxTF(p, it);
    long collectionLength = p.getLong("collectionLength");
    double cp = p.getDouble("collectionProbability");
    long documentCount = p.getLong("documentCount");
    //int avgDocLength = (int) Math.round((collectionLength + 0.0) / (documentCount + 0.0));
    int avgDocLength = 1200; /// fuckin...UGH
    min = function.score(0, avgDocLength); // Allows for a slightly more conservative "worst-case"
  }

  @Override
  public double minimumScore() {
    return min;
  }

  @Override
  public double getWeight() {
    return weight;
  }

  @Override
  public void deltaScore(int count, int length) {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    
    double diff = weight * (function.score(count, length) - max);
    ctx.runningScore += diff;
  }

  @Override
  public void deltaScore(int length) {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double diff = weight * (function.score(count, length) - max);
    
    ctx.runningScore += diff;
  }

  @Override
  public void deltaScore() {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    int count = 0;

    if (iterator.hasMatch(context.document)) {
      count = ((CountIterator) iterator).count();
    }

    double diff = weight * (function.score(count, context.getLength()) - max);
    if (context.document == 12110526) {
	System.err.printf("DELTA : %s -> match=%b, cand=%d, l=%d, c=%d, max=%f, weight=%f, score=%f, delta=%f\n",
			  Utility.shortName(this), iterator.hasMatch(context.document), 
			  iterator.currentCandidate(), context.getLength(), count, max, weight,
			  function.score(count, context.getLength()), diff);
    }
    ctx.runningScore += diff;
  }

  @Override
  public void maximumDifference() {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    double diff = weight * (min - max);
    ctx.runningScore += diff;
    ////CallTable.increment("aux_flops");
  }

  @Override
  public void setContext(ScoringContext ctx) {
    super.setContext(ctx);
    if (DeltaScoringContext.class.isAssignableFrom(ctx.getClass())) {
      DeltaScoringContext dctx = (DeltaScoringContext) ctx;
      dctx.scorers.add(this);
      dctx.startingPotential += (max * weight);
      ////CallTable.increment("aux_flops");
    }
  }

  @Override
  public void aggregatePotentials(DeltaScoringContext ctx) {
    // Nothing to do for this one
  }
}
