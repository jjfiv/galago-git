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

  public JelinekMercerProbabilityScoringIterator(NodeParameters p, 
          MovableLengthsIterator ls, MovableCountIterator it)
          throws IOException {
    super(p, ls, it);
    this.setScoringFunction(new JelinekMercerProbabilityScorer(p, it));
    partName = p.getString("lengths");
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
