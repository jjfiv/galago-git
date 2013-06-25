// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * Implements an iterator over the BM25RF scoring mechanism based on TSV
 * (term-selection value) fitness. This comes from some work by Stephen
 * Robertson and others, I believe in TREC 2004 or so. Obviously, don't quote me
 * on that. A decent review of the original TSV algorithm was done by Billerbeck
 * and Zobel in using short document summaries for fast query expansion.
 *
 * @author irmarc
 */
public class BM25RFScoringIterator extends ScoringFunctionIterator {

  private final double value;

  public BM25RFScoringIterator(NodeParameters np, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(np, ls, it);


    int rt = (int) np.get("rt", 0);
    int R = (int) np.get("R", 0);
    long N = np.getLong("documentCount");
    double factor = np.get("factor", 0.33D);
    // now get idf
    long ft = 0;
    if (np.containsKey("ft")) {
      ft = (int) np.get("ft", 0);
    } else {
      ft = iterator.totalEntries();
    }
    assert (ft >= rt); // otherwise they're wrong and/or lying
    double numerator = (rt + 0.5) / (R - rt + 0.5);
    double denominator = (ft - rt + 0.5) / (N - ft - R + rt + 0.5);

    value = factor * Math.log(numerator / denominator);
  }

  /**
   * We override the score method here b/c the superclass version will always
   * call score, but with a 0 count, in case the scorer smoothes. In this case,
   * the count and length are irrelevant, and it's matching on the identifier
   * list that matters.
   *
   * @return
   */
  @Override
  public double score(ScoringContext c) {
    if (iterator.currentCandidate() == c.document) {
      return value;
    } else {
      return 0;
    }
  }

  /**
   * For this particular scoring function, the parameters are irrelevant. Always
   * returns the predetermined boosting score.
   *
   * @return
   */
  @Override
  public double maximumScore() {
    return value;
  }

  /**
   * For this particular scoring function, the parameters are irrelevant Always
   * returns the predetermined boosting score.
   *
   * @return
   */
  @Override
  public double minimumScore() {
    return value;
  }
}
