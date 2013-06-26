// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;

/**
 *
 * A ScoringIterator that makes use of the DirichletScorer function for
 * converting a count into a score.
 *
 * @author irmarc, sjh
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "nodeFrequency", "maximumCount", "maxLength"})
@RequiredParameters(parameters = {"mu"})
public class DirichletTrueScoringIterator extends ScoringFunctionIterator {

  // delta
  double weight;
  double min; // min score
  double max; // max tf
  // stats
  private final double mu;
  private final double background;
  private final long collectionFrequency;

  public DirichletTrueScoringIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);

    // stats
    mu = p.get("mu", 1500D);
    long collectionLength = p.getLong("collectionLength");
    collectionFrequency = p.getLong("nodeFrequency");
    background = (collectionFrequency > 0)
            ? (double) collectionFrequency / (double) collectionLength
            : 0.5 / (double) collectionLength;

    // delta
    weight = p.get("w", 1.0);

    // the max score can be bounded where the maxtf is also the length of that document (a long document of just tf)
    max = dirichletScore(p.getLong("maximumCount"), p.getLong("maximumCount"));

    // the min score can be bounded at the longest document that does not contain the term:
    min = dirichletScore(0, p.getLong("maxLength")+1);
  }

  /**
   * Underestimate of score
   */
  @Override
  public double minimumScore() {
    return min;
  }

  /**
   * Overestimate of score
   */
  @Override
  public double maximumScore() {
    return max;
  }

  @Override
  public double score(ScoringContext c) {
    int count = ((CountIterator) iterator).count(c);
    int length = this.lengthsIterator.length(c);
    return dirichletScore(count, length);
  }

  private double dirichletScore(double count, double length) {
    double numerator = count + (mu * background);
    double denominator = length + mu;
    return Math.log(numerator / denominator);
  }
}
