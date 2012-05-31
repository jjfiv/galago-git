// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.BM25Scorer;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "nodeDocumentCount"})
@RequiredParameters(parameters = {"b", "k"})
public class BM25ScoringIterator extends ScoringFunctionIterator {

  public BM25ScoringIterator(NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(p,it, new BM25Scorer(p, it));
  }

  /**
   * Score is maxed by having count = length, but having both numbers be as high
   * as possible.
   *
   * @return
   */
  @Override
  public double maximumScore() {
    return function.score(Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

  /**
   * Minimized by having no occurrences.
   *
   * @return
   */
  @Override
  public double minimumScore() {
    return function.score(0, Integer.MAX_VALUE);
  }
}
