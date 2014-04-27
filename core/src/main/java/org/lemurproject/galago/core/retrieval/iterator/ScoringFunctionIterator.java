// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An iterator that converts a count iterator's count into a score. This is
 * usually composed w/ a scoring function in order to produce an appropriate
 * score
 *
 * @author irmarc
 */
public abstract class ScoringFunctionIterator extends TransformIterator implements ScoreIterator {

  protected NodeParameters np;
  protected LengthsIterator lengthsIterator;
  protected CountIterator countIterator;

  public ScoringFunctionIterator(NodeParameters np, 
          LengthsIterator lengths,
          CountIterator iterator) throws IOException {
    super(iterator);
    this.np = np;
    this.lengthsIterator = lengths;
    this.countIterator = iterator;
  }

  /**
   * Over the lengths iterator trails the counts iterator.
   * When 'syncTo' is called, the lengths iterator catches up.
   */
  @Override
  public void syncTo(long document) throws IOException{
    super.syncTo(document);
    this.lengthsIterator.syncTo(document);
  }
  
  @Override
  public double maximumScore() {
    return Double.POSITIVE_INFINITY;
  }

  @Override
  public double minimumScore() {
    return Double.NEGATIVE_INFINITY;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = Double.toString(score(c));
    List<AnnotatedNode> children = new ArrayList<AnnotatedNode>();
    children.add(this.lengthsIterator.getAnnotatedNode(c));
    children.add(this.countIterator.getAnnotatedNode(c));

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
