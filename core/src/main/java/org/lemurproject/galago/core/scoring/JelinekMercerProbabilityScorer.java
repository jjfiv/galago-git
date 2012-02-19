// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import org.galagosearch.core.retrieval.iterator.CountValueIterator;
import org.galagosearch.core.retrieval.query.NodeParameters;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.tupleflow.Parameters;

/**
 * Does not log. Returns actual probability.
 * 
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionProbability"})
public class JelinekMercerProbabilityScorer implements ScoringFunction {

  double background;
  double lambda;

  public JelinekMercerProbabilityScorer(Parameters globalParameters, NodeParameters parameters, CountValueIterator iterator) throws IOException {

    lambda = parameters.get("lambda", globalParameters.get("lambda", 0.5D));
    background = parameters.getDouble("collectionProbability");
  }

  public double score(int count, int length) {
    if (length > 0) {
      double foreground = (double) count / (double) length;
      return (lambda * foreground) + ((1 - lambda) * background);
    } else {
      return (1-lambda) * background;
    }
  }
}
