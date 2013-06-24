// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author marc
 */
public class BinaryCountIterator extends TransformIterator implements CountIterator {

  NodeParameters np;
  IndicatorIterator indicator;

  public BinaryCountIterator(NodeParameters p, IndicatorIterator i) {
    super(i);
    np = p;
    indicator = i;
  }

  @Override
  public int count() {
    return (indicator.indicator(this.currentCandidate())) ? 1 : 0;
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "count";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Integer.toString(count());
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode());

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
