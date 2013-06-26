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

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "nodeFrequency", "nodeDocumentCount", "maximumCount"})
@RequiredParameters(parameters = {"b", "k"})
public class BM25ScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  // delta
  double weight;
  double min;
  double max;
  // scoring
  private final double b;
  private final double k;
  private final long documentCount;
  private final double avgDocLength;
  private final double idf;
  private final long collectionFrequency;

  public BM25ScoringIterator(NodeParameters np, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(np, ls, it);

    // statistics for BM25 scoring
    b = np.get("b", 0.75);
    k = np.get("k", 1.2);

    double collectionLength = np.getLong("collectionLength");
    collectionFrequency = np.getLong("nodeFrequency");
    documentCount = np.getLong("documentCount");
    avgDocLength = (collectionLength + 0.0) / (documentCount + 0.0);

    // now get idf
    long df = np.getLong("nodeDocumentCount");
    // I'm not convinced this is the correct idf formulation -- MAC
    //idf = Math.log((documentCount - df + 0.5) / (df + 0.5));

    idf = Math.log(documentCount / (df + 0.5));

    // Delta scoring stuff
    weight = np.get("w", 1.0);
    max = np.getLong("maximumCount");
    min = score(0, np.getLong("maximumCount"));
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
    double diff = weight * (score(c) - max);
    return diff;
  }

  @Override
  public double maximumDifference() {
    double diff = weight * (min - max);
    return diff;
  }

  @Override
  public double maximumWeightedScore() {
    return max * weight;
  }

  @Override
  public double minimumWeightedScore() {
    return min * weight;
  }
  /**
   * Scoring function interface (allows direct scoring)
   * @return 
   */
  @Override
  public double score(ScoringContext c) {
    double count = ((CountIterator) iterator).count(c);
    double length = this.lengthsIterator.length(c);

    return score(count, length);
  }

  private double score(double count, double length) {
    double numerator = count * (k + 1);
    double denominator = count + (k * (1 - b + (b * length / avgDocLength)));
    return idf * numerator / denominator;
  }
}
