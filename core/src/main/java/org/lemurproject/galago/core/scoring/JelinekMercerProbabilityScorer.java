// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;

/**
 * Does not log. Returns actual probability.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeFrequency","collectionLength"})
@RequiredParameters(parameters = {"lambda"})
public class JelinekMercerProbabilityScorer implements ScoringFunction {

  double background;
  double lambda;

  public JelinekMercerProbabilityScorer(NodeParameters parameters, MovableCountIterator iterator) throws IOException {

    lambda = parameters.get("lambda", 0.5D);
    long collectionLength = parameters.getLong("collectionLength");
    long collectionFrequency = parameters.getLong("nodeFrequency");
    background = (collectionFrequency > 0)
            ? (double) collectionFrequency / (double) collectionLength
            : 0.5 / (double) collectionLength;
  }

  public double score(int count, int length) {
    if (length > 0) {
      double foreground = (double) count / (double) length;
      return (lambda * foreground) + ((1 - lambda) * background);
    } else {
      return (1 - lambda) * background;
    }
  }
}
