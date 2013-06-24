// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * Implements the #any indicator operator.
 * @author irmarc
 */
public class ExistentialIndicatorIterator extends DisjunctionIterator implements IndicatorIterator {

  public ExistentialIndicatorIterator(NodeParameters p, BaseIterator[] children) {
    super(children);
  }

  @Override
  public boolean indicator(long identifier) {
    for (BaseIterator i : this.iterators) {
      if (!i.isDone() && i.hasMatch(identifier)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getValueString() throws IOException {
    return this.currentCandidate() + " " + this.indicator(this.currentCandidate());
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "indicator";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = Boolean.toString(this.indicator(c.document));
    List<AnnotatedNode> children = new ArrayList();
    for (BaseIterator child : this.iterators) {
      children.add(child.getAnnotatedNode(c));
    }
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
