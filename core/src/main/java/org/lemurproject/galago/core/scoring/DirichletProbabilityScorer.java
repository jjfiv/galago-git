// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Does not log. Returns the actual probability.
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionProbability"})
public class DirichletProbabilityScorer implements ScoringFunction {

  double background;
  double mu;

  public DirichletProbabilityScorer(Parameters globalParams, NodeParameters parameters, MovableCountIterator iterator) throws IOException {

    mu = parameters.get("mu", globalParams.get("mu", 1500D));
    background = parameters.getDouble("collectionProbability");
  }

  public double score(int count, int length) {
    double numerator = count + (mu * background);
    double denominator = length + mu;
    return (numerator / denominator);
  }
}
