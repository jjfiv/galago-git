// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "nodeFrequency", "nodeDocumentCount", "maximumCount"})
@RequiredParameters(parameters = {"b", "k"})
public class BM25ScoringIterator extends ScoringFunctionIterator implements DeltaScoringIterator {

  // delta
  private final double weight;
  private final double min;
  private final double max;
  private final double weightedMax;
  private final double weightedMin;
  private final double weightedMaxDiff;
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
    max = score(np.getLong("maximumCount"), np.getLong("maximumCount"));
    min = score(0, 1);

    weightedMin = weight * min;
    weightedMax = weight * max;
    weightedMaxDiff = weightedMax - weightedMin;
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
    double diff = weight * (max - score(c));
    return diff;
  }

  /**
   * Scoring function interface (allows direct scoring)
   *
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

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "bm25";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = Double.toString(score(c));
    List<AnnotatedNode> children = new ArrayList<AnnotatedNode>();
    children.add(this.lengthsIterator.getAnnotatedNode(c));
    children.add(this.countIterator.getAnnotatedNode(c));
    String extraInfo = "idf="+idf;
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, extraInfo, children);
  }
}
