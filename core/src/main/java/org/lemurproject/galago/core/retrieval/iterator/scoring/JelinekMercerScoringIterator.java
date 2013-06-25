// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.iterator.scoring.JelinekMercerScorer;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"maximumCount", "collectionLength", "nodeFrequency"})
@RequiredParameters(parameters = {"lambda"})
public class JelinekMercerScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  // delta
  double weight;
  double min;
  double max;
  // stats
  private final double lambda;
  private final double background;

  public JelinekMercerScoringIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);

    // stats
    lambda = p.get("lambda", 0.5D);
    long collectionLength = p.getLong("collectionLength");
    long collectionFrequency = p.getLong("nodeFrequency");
    background = (collectionFrequency > 0)
            ? (double) collectionFrequency / (double) collectionLength
            : 0.5 / (double) collectionLength;

    // delta
    weight = p.get("w", 1.0);
    max = p.getLong("maximumCount");
    min = score(0, (int) p.getLong("maximumCount"));

  }

  @Override
  public double minimumScore() {
    return min;
  }

  @Override
  public double maximumScore() {
    return max;
  }

  public double getWeight() {
    return weight;
  }

  @Override
  public double deltaScore(ScoringContext c) {
    int count = ((CountIterator) iterator).count(c);
    int length = this.lengthsIterator.length(c);
    double diff = weight * (score(count, length) - max);
    return diff;
  }

  @Override
  public double maximumDifference() {
    double diff = weight * (min - max);
    return diff;
  }

  @Override
  public double startingPotential() {
    return max * weight;
  }

  public double score(double count, double length) {
    double foreground = (double) count / (double) length;
    return Math.log((lambda * foreground) + ((1 - lambda) * background));
  }

  @Override
  public double score(ScoringContext c) {
    int count = ((CountIterator) iterator).count(c);
    int length = this.lengthsIterator.length(c);
    return score(count, length);
  }
}
