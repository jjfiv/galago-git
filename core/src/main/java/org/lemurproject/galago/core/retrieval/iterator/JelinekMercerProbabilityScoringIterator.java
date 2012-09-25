// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.JelinekMercerProbabilityScorer;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength","nodeFrequency"})
@RequiredParameters(parameters = {"lambda"})
public class JelinekMercerProbabilityScoringIterator extends ScoringFunctionIterator {

  protected double loweredMaximum = Double.POSITIVE_INFINITY;
  protected String partName;

  public JelinekMercerProbabilityScoringIterator(NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(p, it, new JelinekMercerProbabilityScorer(p, it));
    partName = p.getString("lengths");
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

  /**
   * Maximize the probability
   *
   * @return
   */
  @Override
  public double maximumScore() {
    if (loweredMaximum != Double.POSITIVE_INFINITY) {
      return loweredMaximum;
    } else {
      return function.score(1, 1);
    }
  }

  @Override
  public double minimumScore() {
    return function.score(0, 1);
  }
}
