// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.DirichletScorer;

/**
 *
 * A ScoringIterator that makes use of the DirichletScorer function for
 * converting a count into a score.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionProbability"})
@RequiredParameters(parameters = {"mu"})
public class DirichletScoringIterator extends ScoringFunctionIterator {

  public DirichletScoringIterator(NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(p, it, new DirichletScorer(p, it));
  }

  @Override
  public double maximumScore() {
    return function.score(Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

  @Override
  public double minimumScore() {
    return function.score(0, Integer.MAX_VALUE);
  }
}
