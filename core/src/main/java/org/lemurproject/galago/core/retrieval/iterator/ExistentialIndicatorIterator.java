// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
  public boolean indicator(int identifier) {
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
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "indicator";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Boolean.toString(this.indicator(this.context.document));
    List<AnnotatedNode> children = new ArrayList();
    for (BaseIterator child : this.iterators) {
      children.add(child.getAnnotatedNode());
    }
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
