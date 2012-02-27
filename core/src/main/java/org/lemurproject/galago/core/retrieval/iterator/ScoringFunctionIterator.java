// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.scoring.ScoringFunction;
import org.lemurproject.galago.core.util.CallTable;

/**
 * An iterator that converts a count iterator's count into a score.
 * This is usually composed w/ a scoring function in order to produce an
 * appropriate score
 *
 * @author irmarc
 */
public class ScoringFunctionIterator extends TransformIterator implements MovableScoreIterator {

  protected ScoringFunction function;

  public ScoringFunctionIterator(MovableCountIterator iterator, ScoringFunction function) throws IOException {
    super(iterator);
    this.function = function;
  }

  public ScoringFunction getScoringFunction() {
    return function;
  }

  @Override
  public double score(ScoringContext dc) {
    int count = 0;

    // Used in counting # of score calls. Uncomment if you want to track that.
    CallTable.increment("score_req");
    if (iterator.currentCandidate() == dc.document) {
      count = ((CountIterator)iterator).count();
    }
    return function.score(count, dc.length);
  }

  @Override
  public double score() {
    int count = 0;

    // Used in counting # of score calls. Uncomment if you want to track that.
    CallTable.increment("score_req");
    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator)iterator).count();
    }
    double score = function.score(count, context.length);
    return score;
  }

  @Override
  public double maximumScore() {
    return Double.POSITIVE_INFINITY;
  }

  @Override
  public double minimumScore() {
    return Double.NEGATIVE_INFINITY;
  }
}
