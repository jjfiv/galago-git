// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * A Dirichlet smoothing node, applied over a raw term count node.
 *
 * @author trevor, irmarc
 */
@RequiredStatistics(statistics = {"collectionProbability"})
@RequiredParameters(parameters = {"mu"})
public class DirichletScorer implements ScoringFunction {

  double background;
  double mu;

  public DirichletScorer(Parameters globalParams, NodeParameters parameters, MovableCountIterator iterator) throws IOException {

    mu = parameters.get("mu", globalParams.get("mu", 1500D));
    background = parameters.getDouble("collectionProbability");
  }

  public double score(int count, int length) {
    double numerator = count + (mu * background);
    double denominator = length + mu;
    return Math.log(numerator / denominator);
  }

  public double score(int count, int length, double externalBk) {
    double numerator = count + (mu * externalBk);
    double denominator = length + mu;
    return Math.log(numerator / denominator);
  }

  public double getBackground() {
    return background;
  }

  public void setBackground(double bk) {
    background = bk;
  }
}
