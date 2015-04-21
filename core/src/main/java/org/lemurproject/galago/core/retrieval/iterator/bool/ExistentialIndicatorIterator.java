// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.bool;

import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.DisjunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements the #any, #bor indicator operator.
 * @author irmarc
 */
public class ExistentialIndicatorIterator extends DisjunctionIterator implements IndicatorIterator {

  public ExistentialIndicatorIterator(NodeParameters p, IndicatorIterator[] children) {
    super(children);
  }

  @Override
  public boolean indicator(ScoringContext c) {
    for (BaseIterator it : this.iterators) {
      IndicatorIterator iter = (IndicatorIterator) it;
      if (iter.indicator(c)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getValueString(ScoringContext c) throws IOException {
    return this.currentCandidate() + " " + this.indicator(c);
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "indicator";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = Boolean.toString(indicator(c));
    List<AnnotatedNode> children = new ArrayList<>();
    for (BaseIterator child : this.iterators) {
      children.add(child.getAnnotatedNode(c));
    }
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
