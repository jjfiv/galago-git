// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author trevor
 */
public class ScaleIterator extends TransformIterator {
  NodeParameters np;
  double weight;

  public ScaleIterator(NodeParameters parameters, ScoreIterator iterator) throws IllegalArgumentException {
    super(iterator);
    this.np = parameters;
    weight = parameters.get("default", 1.0);
  }

  public double score() {
    return weight * ((ScoreIterator) iterator).score();
  }

  public double maximumScore() {
    return ((ScoreIterator) iterator).maximumScore();
  }

  public double minimumScore() {
    return ((ScoreIterator) iterator).minimumScore();
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

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
