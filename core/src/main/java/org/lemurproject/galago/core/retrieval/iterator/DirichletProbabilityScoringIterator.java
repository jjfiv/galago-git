/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.DirichletProbabilityScorer;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * A ScoringIterator that makes use of the DirichletScorer function for
 * converting a count into a score.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "collectionProbability"})
@RequiredParameters(parameters = {"mu"})
public class DirichletProbabilityScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  private double weight;
  public double min;
  private int parentIdx;
  String partName;

  public DirichletProbabilityScoringIterator(NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(p, it, new DirichletProbabilityScorer(p, it));
    max = getMaxTF(p, it);
    partName = p.getString("lengths");
    parentIdx = (int) p.getLong("pIdx");
    weight = p.getDouble("w");
    long collectionLength = p.getLong("collectionLength");
    long documentCount = p.getLong("documentCount");
    int avgDocLength = (int) Math.round(1.5 * (collectionLength + 0.0) / (documentCount + 0.0)); // Increase doc size by 50%
    min = function.score(0, avgDocLength); // Allows for a slightly more conservative "worst-case"
  }

  @Override
  public double minimumScore() {
    return min;
  }

  public double getWeight() {
    return weight;
  }

  @Override
  public void deltaScore(int count, int length) {
    DeltaScoringContext ctx = (DeltaScoringContext) context;

    double diff = weight * (function.score(count, length) - max);
    double newValue = ctx.potentials[parentIdx] + diff;

    ctx.runningScore += Math.log(newValue / ctx.potentials[parentIdx]);
    ctx.potentials[parentIdx] = newValue;

  }

  @Override
  public void deltaScore(int length) {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double diff = weight * (function.score(count, length) - max);
    double newValue = ctx.potentials[parentIdx] + diff;

    ctx.runningScore += Math.log(newValue / ctx.potentials[parentIdx]);
    ctx.potentials[parentIdx] = newValue;

  }

  @Override
  public void deltaScore() {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double diff = weight * (function.score(count, context.getLength(partName)) - max);
    double newValue = ctx.potentials[parentIdx] + diff;

    ctx.runningScore += Math.log(newValue / ctx.potentials[parentIdx]);
    ctx.potentials[parentIdx] = newValue;

  }

  @Override
  public double score() {
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }
    double score = function.score(count, context.getLength(partName));

    return score;
  }

  @Override
  public void maximumDifference() {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    double diff = weight * (min - max);
    double newValue = ctx.potentials[parentIdx] + diff;

    ctx.runningScore += Math.log(newValue / ctx.potentials[parentIdx]);
    ctx.potentials[parentIdx] = newValue;
  }

  @Override
  public void setContext(ScoringContext ctx) {
    super.setContext(ctx);
    if (DeltaScoringContext.class.isAssignableFrom(ctx.getClass())) {
      DeltaScoringContext dctx = (DeltaScoringContext) ctx;
      dctx.scorers.add(this);
      dctx.startingPotentials[parentIdx] += (max * weight);
    }
  }

  @Override
  public void aggregatePotentials(DeltaScoringContext ctx) {
    for (double d : ctx.startingPotentials) {
      ctx.startingPotential += Math.log(d);
    }
  }
}
