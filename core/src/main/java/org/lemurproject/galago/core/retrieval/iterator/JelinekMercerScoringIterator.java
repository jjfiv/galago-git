// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.TopDocsReader.TopDocument;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.processing.TopDocsContext;
import org.lemurproject.galago.core.scoring.JelinekMercerScorer;
import org.lemurproject.galago.tupleflow.Parameters;


/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionProbability"})
public class JelinekMercerScoringIterator extends ScoringFunctionIterator {
    protected double loweredMaximum = Double.POSITIVE_INFINITY;


    public JelinekMercerScoringIterator(Parameters globalParams, NodeParameters p, CountValueIterator it)
            throws IOException {
    super(it, new JelinekMercerScorer(globalParams, p, it));
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
        TopDocument worst = tdc.hold.get(tdc.hold.size()-1);
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
      return function.score(1,1);
    }
  }

  public double minimumScore() {
      return function.score(0,1);
  }
}
