// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.disk.TopDocsReader.TopDocument;
import org.lemurproject.galago.core.retrieval.processing.FieldScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.processing.TopDocsContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.JelinekMercerProbabilityScorer;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionProbability"})
public class JelinekMercerProbabilityScoringIterator extends ScoringFunctionIterator {

  protected double loweredMaximum = Double.POSITIVE_INFINITY;
  protected String partName;

  public JelinekMercerProbabilityScoringIterator(Parameters globalParams, NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(it, new JelinekMercerProbabilityScorer(globalParams, p, it));
    partName = p.getString("lengths");
  }

  @Override
  public double score() {
    if (FieldScoringContext.class.isAssignableFrom(context.getClass())) {
      int count = 0;

      if (iterator.currentCandidate() == context.document) {
        count = ((CountIterator) iterator).count();
      }
      double score = function.score(count, ((FieldScoringContext) context).lengths.get(partName));
      return score;
    } else {
      return super.score();
    }
  }

    /**
     * Overriding this in case we're using topdocs, in which case, also drop the
     * maximum once vs multiple times.
     */
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
   * @return
   */
  public double maximumScore() {
    if (loweredMaximum != Double.POSITIVE_INFINITY) {
      return loweredMaximum;
    } else {
      return function.score(1, 1);
    }
  }

  public double minimumScore() {
    return function.score(0, 1);
  }
}
