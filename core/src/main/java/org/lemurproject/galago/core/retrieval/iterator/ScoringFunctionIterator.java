// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.scoring.ScoringFunction;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * An iterator that converts a count iterator's count into a score.
 * This is usually composed w/ a scoring function in order to produce an
 * appropriate score
 *
 * @author irmarc
 */
public class ScoringFunctionIterator extends TransformIterator implements MovableScoreIterator {

  protected NodeParameters np;
  protected ScoringFunction function;
  protected CountIterator countIterator;
  protected double max;

  public ScoringFunctionIterator(NodeParameters np, MovableCountIterator iterator, ScoringFunction function) throws IOException {
    super(iterator);
    this.np = np;
    this.function = function;
    this.countIterator = iterator;
  }

  public ScoringFunction getScoringFunction() {
    return function;
  }

  @Override
  public double score() {
    int count = 0;

    if (iterator.hasMatch(context.document)) {
      count = countIterator.count();
    }
    double score = function.score(count, context.getLength());
    if (context.document == 12038803) {
	System.err.printf("NORMAL: %s -> match=%b, cand=%d, l=%d, c=%d, score=%f\n",
			  Utility.shortName(this), iterator.hasMatch(context.document),
			  iterator.currentCandidate(), context.getLength(), count,
			  score);
    }
    return score;
  }

  @Override
  public double maximumScore() {
    return max;
  }

  @Override
  public double minimumScore() {
    return Double.NEGATIVE_INFINITY;
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Double.toString(score());
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode());
    String extra = Integer.toString(context.getLength());

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, extra, children);
  }

  /**
   * Non-essential method to forward the count iterator key.
   * @return 
   */
  public byte[] key() {
    return ((CountIterator) iterator).key();
  }
  
  public double getMaxTF(NodeParameters p, MovableCountIterator it) {
    int count = 0;
    if (p.containsKey("maximumCount")) {
      count = (int) p.getLong("maximumCount");
    } else if (it != null) {
      count = it.maximumCount();
    } else {
      return Double.POSITIVE_INFINITY;
    }

    // We have a non-zero number
    return function.score(count, count);
  }
}
