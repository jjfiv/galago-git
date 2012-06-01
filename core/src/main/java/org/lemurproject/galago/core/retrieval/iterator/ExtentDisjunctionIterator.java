/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class ExtentDisjunctionIterator extends DisjunctionIterator implements MovableDataIterator<ExtentArray>, ExtentIterator, MovableCountIterator {

  protected ExtentArray extents;

  public ExtentDisjunctionIterator(MovableExtentIterator[] iterators) throws IOException {
    super(iterators);
    this.extents = new ExtentArray();
  }

  @Override
  public void moveTo(int identifier) throws IOException {
    super.moveTo(identifier);

    extents.reset();
    // check if all iterators are at the same document
    if (!isDone() && super.hasMatch(this.currentCandidate())) {
      // if so : load some extents
      loadExtents();
    }
  }

  @Override
  public boolean hasMatch(int identifier) {
    return super.hasMatch(identifier) && this.extents.size() > 0;
  }

  @Override
  public String getEntry() throws IOException {
    ArrayList<String> strs = new ArrayList<String>();
    ExtentArrayIterator eai = new ExtentArrayIterator(extents);
    while (!eai.isDone()) {
      strs.add(String.format("[%d, %d]", eai.currentBegin(), eai.currentEnd()));
      eai.next();
    }
    return Utility.join(strs.toArray(new String[0]), ",");
  }

  @Override
  public ExtentArray getData() {
    return extents;
  }

  @Override
  public ExtentArray extents() {
    return extents;
  }

  @Override
  public int count() {
    return extents.size();
  }

  @Override
  public int maximumCount() {
    int sum = 0;
    for (int i = 0; i < iterators.length; i++) {
      sum += ((MovableCountIterator) iterators[i]).maximumCount();
    }
    return sum;

  }

  public abstract void loadExtents();

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "extent";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = extents.toString();
    List<AnnotatedNode> children = new ArrayList();
    for (MovableIterator child : this.iterators) {
      children.add(child.getAnnotatedNode());
    }
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
