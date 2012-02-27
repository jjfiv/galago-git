// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * We only land on docs that the indicator allows, otherwise we consider it a miss.
 * Otherwise, all methods simply poll the counter.
 * @author irmarc
 */
public class RequireIterator extends FilteredIterator {

  public RequireIterator(Parameters globalParams, NodeParameters p, MovableIndicatorIterator indicator,
          MovableCountIterator counter) throws IOException {
    super(globalParams, p, indicator, counter);
    moveTo(0);
  }

  public RequireIterator(Parameters globalParams, NodeParameters p, MovableIndicatorIterator indicator,
          MovableScoreIterator scorer) throws IOException {
    super(globalParams, p, indicator, scorer);
    moveTo(0);
  }

  public RequireIterator(Parameters globalParams, NodeParameters p, MovableIndicatorIterator indicator,
          ExtentValueIterator extents) throws IOException {
    super(globalParams, p, indicator, extents);
    moveTo(0);
  }

  public boolean atCandidate(int identifier) {
    return this.mover.atCandidate(identifier)
            && this.indicator.indicator(identifier);
  }
}
