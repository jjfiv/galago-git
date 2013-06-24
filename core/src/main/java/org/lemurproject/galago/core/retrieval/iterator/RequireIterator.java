// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * We only land on docs that the indicatorItr allows, otherwise we consider it a
 * miss. Otherwise, all methods simply poll the counter.
 *
 * @author irmarc
 */
public class RequireIterator extends FilteredIterator {

  public RequireIterator(NodeParameters p, IndicatorIterator indicator,
          CountIterator counter) throws IOException {
    super(p, indicator, counter);
    syncTo(0);
  }

  public RequireIterator(NodeParameters p, IndicatorIterator indicator,
          ScoreIterator scorer) throws IOException {
    super(p, indicator, scorer);
    syncTo(0);
  }

  public RequireIterator(NodeParameters p, IndicatorIterator indicator,
          ExtentIterator extents) throws IOException {
    super(p, indicator, extents);
    syncTo(0);
  }

  @Override
  protected boolean indication(ScoringContext c) {
    return indicatorItr.indicator(c);
  }
}
