// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.bool;

import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.TransformIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
  public int count(ScoringContext c) {
    return (indicator.indicator(c)) ? 1 : 0;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "count";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = Integer.toString(count(c));
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode(c));

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public boolean indicator(ScoringContext c) {
    return indicator.indicator(c);
  }
}
