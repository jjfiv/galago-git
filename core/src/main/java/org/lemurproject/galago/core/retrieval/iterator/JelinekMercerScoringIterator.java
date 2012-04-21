// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.disk.TopDocsReader.TopDocument;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.processing.TopDocsContext;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.scoring.JelinekMercerScorer;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionProbability"})
@RequiredParameters(parameters = {"lambda"})
public class JelinekMercerScoringIterator extends ScoringFunctionIterator {

  protected double loweredMaximum = Double.POSITIVE_INFINITY;

  public JelinekMercerScoringIterator(NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(it, new JelinekMercerScorer(p, it));
  }

  /**
   * Overriding this in case we're using topdocs, in which case, also drop the
   * maximum once vs multiple times.
   */
  @Override
  public void setContext(ScoringContext context) {
    if (TopDocsContext.class.isAssignableFrom((context.getClass()))) {
      TopDocsContext tdc = (TopDocsContext) context;
      if (tdc.hold != null) {
        tdc.topdocs.put(this, tdc.hold);
        TopDocument worst = tdc.hold.get(tdc.hold.size() - 1);
        loweredMaximum = this.function.score(worst.count, worst.length);
        tdc.hold = null;
      }
    }
    this.context = context;
  }

  /**
   * Maximize the probability
   *
   * @return
   */
  @Override
  public double maximumScore() {
    if (loweredMaximum != Double.POSITIVE_INFINITY) {
      return loweredMaximum;
    } else {
      return function.score(1, 1);
    }
  }

  @Override
  public double minimumScore() {
    return function.score(0, 1);
  }
}
