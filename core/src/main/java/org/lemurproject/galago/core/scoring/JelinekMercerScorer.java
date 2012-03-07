// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Jelinek-Mercer smoothing node, applied over raw counts.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionProbability"})
public class JelinekMercerScorer implements ScoringFunction {

  double background;
  double lambda;

  public JelinekMercerScorer(Parameters globalParameters, NodeParameters parameters, MovableCountIterator iterator) throws IOException {

    lambda = parameters.get("lambda", globalParameters.get("lambda", 0.5D));
    background = parameters.getDouble("collectionProbability");
  }

  public double score(int count, int length) {
    double foreground = (double) count / (double) length;
    return Math.log((lambda * foreground) + ((1 - lambda) * background));
  }
}
