/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DisjointCountsIterator extends DisjointIndexesIterator implements CountIterator {

  public DisjointCountsIterator(Collection<CountIterator> iterators) {
    super((Collection) iterators);
  }

  @Override
  public int count() {
    return ((CountIterator) head).count();
  }

  @Override
  public int maximumCount() {
    return ((CountIterator) head).maximumCount();
  }

  @Override
  public byte[] key() {
    return Utility.fromString("DisCI");
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "counts";
    String className = this.getClass().getSimpleName();
    String parameters = this.getKeyString();
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Integer.toString(count());
    List<AnnotatedNode> children = new ArrayList();
    for (MovableIterator child : this.allIterators) {
      children.add(child.getAnnotatedNode());
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
