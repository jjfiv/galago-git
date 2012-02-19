/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.galagosearch.core.retrieval.query.NodeParameters;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.core.scoring.InverseDocumentFrequencyScorer;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeDocumentCount", "collectionLength", "documentCount"})
public class InverseDocFrequencyIterator extends ScoringFunctionIterator {
 public InverseDocFrequencyIterator(Parameters globalParams, NodeParameters p, CountValueIterator it)
          throws IOException {
    super(it, new InverseDocumentFrequencyScorer(globalParams, p, it));
    // And now dump it
    iterator = null;
  }

  /**
   * We override the score method here b/c the superclass version will always
   * call score, but with a 0 count, in case the scorer smoothes. In this case,
   * we always match too.
   *
   * @return
   */
  @Override
  public double score() {
      return function.score(0,0);
  }

  /**
   * For this particular scoring function, the parameters are irrelevant.
   * Always returns the predetermined boosting score.
   * @return
   */
  public double maximumScore() {
    return function.score(0, 0);
  }

  /**
   * For this particular scoring function, the parameters are irrelevant
   * Always returns the predetermined boosting score.
   * @return
   */
  public double minimumScore() {
    return function.score(0, 0);
  }

  public void reset() throws IOException {
  }

    // Immediately done
  public boolean isDone() {
      return true;
  }

  
  public int currentCandidate() {
      return Integer.MAX_VALUE;
  }

  public boolean hasMatch(int identifier) {
      return false;
  }

  public boolean next() throws IOException {
      return false;
  }
  
  public boolean moveTo(int identifier) throws IOException {
      return false;
  }

  public void movePast(int identifier) throws IOException {
  }

  public String getEntry() throws IOException {
      return String.format("IDF: %f", function.score(0,0));
  }

  public long totalEntries() {
      return 0;
  }
}
