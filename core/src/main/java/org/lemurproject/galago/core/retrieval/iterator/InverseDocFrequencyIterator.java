/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.InverseDocumentFrequencyScorer;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeDocumentCount", "collectionLength", "documentCount"})
public class InverseDocFrequencyIterator extends ScoringFunctionIterator {

  public InverseDocFrequencyIterator(NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(it, new InverseDocumentFrequencyScorer(p, it));
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
    return function.score(0, 0);
  }

  /**
   * For this particular scoring function, the parameters are irrelevant. Always
   * returns the predetermined boosting score.
   *
   * @return
   */
  @Override
  public double maximumScore() {
    return function.score(0, 0);
  }

  /**
   * For this particular scoring function, the parameters are irrelevant Always
   * returns the predetermined boosting score.
   *
   * @return
   */
  @Override
  public double minimumScore() {
    return function.score(0, 0);
  }

  @Override
  public void reset() throws IOException {
  }

  // Immediately done
  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public int currentCandidate() {
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean atCandidate(int identifier) {
    return false;
  }

  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public void next() throws IOException {
  }

  @Override
  public void moveTo(int identifier) throws IOException {
  }

  @Override
  public void movePast(int identifier) throws IOException {
  }

  @Override
  public String getEntry() throws IOException {
    return String.format("IDF: %f", function.score(0, 0));
  }

  @Override
  public long totalEntries() {
    return 0;
  }
}
