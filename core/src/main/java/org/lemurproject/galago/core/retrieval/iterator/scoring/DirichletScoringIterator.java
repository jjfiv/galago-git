// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.iterator.scoring.DirichletScorer;

/**
 *
 * A ScoringIterator that makes use of the DirichletScorer function for
 * converting a count into a score.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "nodeFrequency", "maximumCount"})
@RequiredParameters(parameters = {"mu"})
public class DirichletScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  // delta
  double weight;
  double min; // min score
  double max; // max tf
  // stats
  private final double mu;
  private final double background;

  public DirichletScoringIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);

    // stats
    mu = p.get("mu", 1500D);
    long collectionLength = p.getLong("collectionLength");
    long collectionFrequency = p.getLong("nodeFrequency");
    background = (collectionFrequency > 0)
            ? (double) collectionFrequency / (double) collectionLength
            : 0.5 / (double) collectionLength;
    
    // delta
    weight = p.get("w", 1.0);
    max = p.getLong("maximumCount");
    //long collectionLength = p.getLong("collectionLength");    
    //long documentCount = p.getLong("documentCount");
    //int avgDocLength = (int) Math.round((collectionLength + 0.0) / (documentCount + 0.0));
    int avgDocLength = 1200; /// ...UGH
    min = score(0, avgDocLength); // Allows for a slightly more conservative "worst-case"

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
    return weight * (score(count, length) - max);
  }

  @Override
  public double maximumDifference() {
    return weight * (min - max);
  }

  @Override
  public double startingPotential() {
    return (max * weight);
  }

  @Override
  public double score(ScoringContext c) {
    int count = ((CountIterator) iterator).count(c);
    int length = this.lengthsIterator.length(c);
    return score(count, length);
  }

  private double score(double count, double length) {
    double numerator = count + (mu * background);
    double denominator = length + mu;
    return Math.log(numerator / denominator);
  }
}
