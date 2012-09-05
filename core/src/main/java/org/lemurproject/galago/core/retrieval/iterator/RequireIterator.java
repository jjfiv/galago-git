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
public class RequireIterator extends FilteredIterator {

  public RequireIterator(NodeParameters p, MovableIndicatorIterator indicator,
          MovableCountIterator counter) throws IOException {
    super(p, indicator, counter);
    moveTo(0);
  }

  public RequireIterator(NodeParameters p, MovableIndicatorIterator indicator,
          MovableScoreIterator scorer) throws IOException {
    super(p, indicator, scorer);
    moveTo(0);
  }

  public RequireIterator(NodeParameters p, MovableIndicatorIterator indicator,
          MovableExtentIterator extents) throws IOException {
    super(p, indicator, extents);
    moveTo(0);
  }

  public boolean hasMatch(int identifier) {
    return this.mover.hasMatch(identifier)
            && this.indicator.indicator(identifier);
  }

  @Override
  public byte[] key() {
    return Utility.fromString("REQ");
  }
}
