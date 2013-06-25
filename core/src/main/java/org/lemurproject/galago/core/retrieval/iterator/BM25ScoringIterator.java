// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
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
  double max;

  public BM25ScoringIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);
    this.setScoringFunction(new BM25Scorer(p, it));
    weight = p.get("w", 1.0);
    parentIdx = (int) p.get("pIdx", 0);
    max = p.getLong("maximumCount");
    min = function.score(0, (int) p.getLong("maximumCount"));
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
    double diff = weight * (function.score(count, length) - max);
    return diff;
  }

  @Override
  public double maximumDifference() {
    double diff = weight * (min - max);
    return diff;
  }

  @Override
  public void setContext(ScoringContext ctx) {
    super.setContext(ctx);
  }

  @Override
  public double startingPotential() {
    return max * weight;
  }
}
