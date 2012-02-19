// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.galagosearch.core.index.TopDocsReader.TopDocument;
import org.galagosearch.core.retrieval.query.NodeParameters;
import org.galagosearch.core.retrieval.structured.FieldScoringContext;
import org.galagosearch.core.retrieval.structured.ScoringContext;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.retrieval.structured.TopDocsContext;
import org.galagosearch.core.scoring.JelinekMercerProbabilityScorer;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionProbability"})
public class JelinekMercerProbabilityScoringIterator extends ScoringFunctionIterator {

  protected double loweredMaximum = Double.POSITIVE_INFINITY;
  protected String partName;

  public JelinekMercerProbabilityScoringIterator(Parameters globalParams, NodeParameters p, CountValueIterator it)
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
