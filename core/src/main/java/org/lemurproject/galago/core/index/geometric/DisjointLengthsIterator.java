/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

/**
 *
 * @author sjh
 */
public class DisjointLengthsIterator extends DisjointIndexesIterator implements LengthsIterator {

  public DisjointLengthsIterator(Collection<LengthsIterator> iterators) {
    super((Collection) iterators);
  }

  @Override
  public int getCurrentLength() {
    if (head != null) {
      return ((LengthsIterator) this.head).getCurrentLength();
    } else {
      throw new RuntimeException("Lengths Iterator is done.");
    }
  }

  @Override
  public int getCurrentIdentifier() {
    if (head != null) {
      return ((LengthsIterator) this.head).getCurrentIdentifier();
    } else {
      throw new RuntimeException("Lengths Iterator is done.");
    }
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "lengths";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Integer.toString(getCurrentLength());
    List<AnnotatedNode> children = new ArrayList();
    for (MovableIterator child : this.allIterators) {
      children.add(child.getAnnotatedNode());
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public byte[] getRegionBytes() {
    if (head != null) {
      return ((LengthsIterator) this.head).getRegionBytes();
    } else {
      throw new RuntimeException("Lengths Iterator is done.");
    }
  }
}
