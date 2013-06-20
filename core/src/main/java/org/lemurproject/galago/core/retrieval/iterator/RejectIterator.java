// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * We only land on docs that the indicator allows, otherwise we consider it a
 * miss. Otherwise, all methods simply poll the counter.
 *
 * @author irmarc
 */
public class RejectIterator extends FilteredIterator {

  public RejectIterator(NodeParameters p, IndicatorIterator indicator,
          CountIterator counter) throws IOException {
    super(p, indicator, counter);
    syncTo(0);
  }

  public RejectIterator(NodeParameters p, IndicatorIterator indicator,
          ScoreIterator scorer) throws IOException {
    super(p, indicator, scorer);
    syncTo(0);
  }

  public RejectIterator(NodeParameters p, IndicatorIterator indicator,
          ExtentIterator extents) throws IOException {
    super(p, indicator, extents);
    syncTo(0);
  }

  public boolean hasMatch(int identifier) {
    return (!this.indicator.indicator(identifier))
            && this.mover.hasMatch(identifier);
  }
}
