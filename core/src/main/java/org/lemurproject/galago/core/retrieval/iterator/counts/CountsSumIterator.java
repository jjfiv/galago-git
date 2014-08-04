package org.lemurproject.galago.core.retrieval.iterator.counts;

import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.DisjunctionIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jfoley
 */
public class CountsSumIterator extends DisjunctionIterator implements CountIterator {

  private final NodeParameters nodeParams;
  private final CountIterator[] countIterators;

  public CountsSumIterator(NodeParameters np, CountIterator[] countIterators) {
    super(countIterators);
    this.countIterators = countIterators;
    this.nodeParams = np;
  }
  @Override
  public int count(ScoringContext c) {
    int sum = 0;
    for (CountIterator child : countIterators) {
      sum += child.count(c);
    }
    return sum;
  }

  @Override
  public String getValueString(ScoringContext sc) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext sc) throws IOException {
    String type = "count";
    String className = this.getClass().getSimpleName();
    String parameters = nodeParams.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(sc.document);
    String returnValue = Integer.toString(count(sc));
    List<AnnotatedNode> children = new ArrayList<>();
    for (BaseIterator child : this.iterators) {
      children.add(child.getAnnotatedNode(sc));
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
