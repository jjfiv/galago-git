// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;

/**
 * A ScoringIterator that makes use of the DirichletScorer function for
 * converting a count into a score.
 *
 * @author sjh
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "nodeFrequency", "maximumCount", "avgLength"})
@RequiredParameters(parameters = {"mu"})
public class DirichletScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  // delta
  private final double weight;
  private final double min; // min score
  private final double max; // max tf
  private final double weightedMin;
  private final double weightedMax;
  private final double weightedMaxDiff;
  // stats
  private final double mu;
  private final double background;
  private final long collectionFrequency;

  public DirichletScoringIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);

    // stats
    mu = p.get("mu", 1500D);
    long collectionLength = p.getLong("collectionLength");
    collectionFrequency = p.getLong("nodeFrequency");
    background = (collectionFrequency > 0)
            ? (double) collectionFrequency / (double) collectionLength
            : 0.5 / (double) collectionLength;

    // delta
    weight = p.get("w", 1.0);

    // the max score can be bounded where the maxtf is also the length of that document (a long document of just tf)
    max = dirichletScore(p.getLong("maximumCount"), p.getLong("maximumCount"));

    // the min score is an over estimate for when the iterator does NOT contain the term (document freq of zero)
    //   MAX-SCORE requires this to be over estimated, otherwise you will lose documents
    //   empirically average document length is a good number (even if its NOT an overestimate of min possible score)
    min = dirichletScore(0, 1);

    weightedMin = weight * min;
    weightedMax = weight * max;
    weightedMaxDiff = weightedMax - weightedMin;
  }

  @Override
  public double collectionFrequency() {
    return collectionFrequency;
  }

  @Override
  public double minimumScore() {
    return min;
  }

  @Override
  public double maximumScore() {
    return max;
  }

  @Override
  public double getWeight() {
    return weight;
  }

  @Override
  public double deltaScore(ScoringContext c) {
    int count = ((CountIterator) iterator).count(c);
    int length = this.lengthsIterator.length(c);
    return weight * (max - dirichletScore(count, length));
  }

  @Override
  public double maximumWeightedScore() {
    return weightedMax;
  }

  @Override
  public double minimumWeightedScore() {
    return weightedMin;
  }

  @Override
  public double maximumDifference() {
    return weightedMaxDiff;
  }

  @Override
  public double score(ScoringContext c) {
    int count = ((CountIterator) iterator).count(c);
    int length = this.lengthsIterator.length(c);
    return dirichletScore(count, length);
  }

  private double dirichletScore(double count, double length) {
    double numerator = count + (mu * background);
    double denominator = length + mu;
    return Math.log(numerator / denominator);
  }
}
