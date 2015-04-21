/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author sjh
 */
public class DisjointLengthsIterator extends DisjointIndexesIterator implements LengthsIterator {

  public DisjointLengthsIterator(Collection<LengthsIterator> iterators) {
    super((Collection) iterators);
  }

  @Override
  public int length(ScoringContext c) {
    if (head != null) {
      return ((LengthsIterator) this.head).length(c);
    } else {
      throw new RuntimeException("Lengths Iterator is done.");
    }
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "lengths";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = Integer.toString(length(c));
    List<AnnotatedNode> children = new ArrayList<>();
    for (BaseIterator child : this.allIterators) {
      children.add(child.getAnnotatedNode(c));
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
