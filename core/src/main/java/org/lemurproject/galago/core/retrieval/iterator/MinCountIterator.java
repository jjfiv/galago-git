/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author sjh
 */
public class MinCountIterator extends ConjunctionIterator implements CountIterator {

  private final NodeParameters nodeParams;
  private final CountIterator[] countIterators;

  public MinCountIterator(NodeParameters np, CountIterator[] countIterators) {
    super(np, countIterators);
    this.countIterators = countIterators;
    this.nodeParams = np;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "count";
    String className = this.getClass().getSimpleName();
    String parameters = nodeParams.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = Integer.toString(count(c));
    List<AnnotatedNode> children = new ArrayList();
    for (BaseIterator child : this.iterators) {
      children.add(child.getAnnotatedNode(c));
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public int count(ScoringContext c) {
    int count = Integer.MAX_VALUE;
    for (CountIterator countItr : countIterators) {
      count = Math.min(count, countItr.count(c));
    }
    count = (count == Integer.MAX_VALUE) ? 0 : count;
    return count;
  }

  @Override
  public void setContext(ScoringContext sc) {
    this.context = sc;

    for (BaseIterator itr : this.iterators) {
      itr.setContext(context);
    }
  }

  @Override
  public String getValueString() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
