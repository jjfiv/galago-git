/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

/**
 *
 * @author sjh
 */
public class DisjointCountsIterator extends DisjointIndexesIterator implements MovableCountIterator {

  public DisjointCountsIterator(Collection<MovableCountIterator> iterators) {
    super((Collection) iterators);
  }

  @Override
  public int count() {
    return ((MovableCountIterator) head).count();
  }

  @Override
  public int maximumCount() {
    return ((MovableCountIterator) head).maximumCount();
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "counts";
    String className = this.getClass().getSimpleName();
    String parameters = this.getKeyString();
    int document = currentCandidate();
    boolean atCandidate = atCandidate(this.context.document);
    String returnValue = Integer.toString(count());
    List<AnnotatedNode> children = new ArrayList();
    for(MovableIterator child : this.allIterators){
      children.add(child.getAnnotatedNode());
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
