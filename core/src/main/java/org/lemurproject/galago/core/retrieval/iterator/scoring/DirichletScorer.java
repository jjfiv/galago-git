// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 * A Dirichlet smoothing node, applied over a raw term count node.
 *
 * @author trevor, irmarc
 */
@RequiredStatistics(statistics = {"nodeFrequency","collectionLength"})
@RequiredParameters(parameters = {"mu"})
public class DirichletScorer {

  double background;
  double mu;

  public DirichletScorer(NodeParameters parameters) throws IOException {

    mu = parameters.get("mu", 1500D);
    long collectionLength = parameters.getLong("collectionLength");
    long collectionFrequency = parameters.getLong("nodeFrequency");
    background = (collectionFrequency > 0)
            ? (double) collectionFrequency / (double) collectionLength
            : 0.5 / (double) collectionLength;
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
