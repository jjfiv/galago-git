// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 *
 * @author irmarc, sjh
 */
@RequiredStatistics(statistics = {"maximumCount", "collectionLength", "nodeFrequency"})
@RequiredParameters(parameters = {"lambda"})
public class JelinekMercerScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  // delta
  private final double weight;
  private final double min;
  private final double max;
  private final double weightedMin;
  private final double weightedMax;
  private final double weightedMaxDiff;
  // stats
  public final double lambda;
  public final double background;
  public final long collectionFrequency;

  public JelinekMercerScoringIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);

    // stats
    lambda = p.get("lambda", 0.5D);
    long collectionLength = p.getLong("collectionLength");
    collectionFrequency = p.getLong("nodeFrequency");
    background = (collectionFrequency > 0)
            ? (double) collectionFrequency / (double) collectionLength
            : 0.5 / (double) collectionLength;

    // delta
    weight = p.get("w", 1.0);
    max = score(p.getLong("maximumCount"), p.getLong("maximumCount"));
    min = score(0, 1);

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
  public double maximumDifference() {
    return weightedMaxDiff;
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
  public double deltaScore(ScoringContext c) {
    int count = ((CountIterator) iterator).count(c);
    int length = this.lengthsIterator.length(c);
    double diff = weight * (max - score(count, length));
    return diff;
  }

  @Override
  public double score(ScoringContext c) {
    int count = ((CountIterator) iterator).count(c);
    int length = this.lengthsIterator.length(c);
    return score(count, length);
  }

  public double score(double count, double length) {
    double foreground = (double) count / (double) length;
    return Math.log((lambda * foreground) + ((1 - lambda) * background));
  }
}
