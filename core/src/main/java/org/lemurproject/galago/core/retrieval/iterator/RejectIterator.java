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
public class RejectIterator extends FilteredIterator {

  public RejectIterator(Parameters globalParams, NodeParameters p, IndicatorIterator indicator,
          CountValueIterator counter) throws IOException {
    super(globalParams, p, indicator, counter);
    moveTo(0);
  }

  public RejectIterator(Parameters globalParams, NodeParameters p, IndicatorIterator indicator,
          ScoreValueIterator scorer) throws IOException {
    super(globalParams, p, indicator, scorer);
    moveTo(0);
  }

  public RejectIterator(Parameters globalParams, NodeParameters p, IndicatorIterator indicator,
          ExtentValueIterator extents) throws IOException {
    super(globalParams, p, indicator, extents);
    moveTo(0);
  }

  public boolean hasMatch(int identifier) {
    return (!this.indicator.hasMatch(identifier))
            && this.mover.hasMatch(identifier);
  }
}
