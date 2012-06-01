/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class ExtentConjunctionIterator extends ConjunctionIterator implements MovableDataIterator<ExtentArray>, ExtentIterator, MovableCountIterator {

  protected ExtentArray extents;

  public ExtentConjunctionIterator(NodeParameters parameters, MovableExtentIterator[] iterators) throws IOException {
    super(parameters, iterators);
    this.extents = new ExtentArray();
  }

  @Override
  public void moveTo(int identifier) throws IOException {
    this.extents.reset();
    // note that this function could repeatedly move child iterators
    super.moveTo(identifier);
  }

  @Override
  public boolean hasMatch(int identifier) {
    if(this.extents.size() == 0){
      this.loadExtents();
    }
    return super.hasMatch(identifier) && this.extents.size() > 0;
  }

  @Override
  public String getEntry() throws IOException {
    if(this.extents.size() == 0){
      this.loadExtents();
    }
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
    if(this.extents.size() == 0){
      this.loadExtents();
    }
    return extents;
  }

  @Override
  public ExtentArray extents() {
    if(this.extents.size() == 0){
      this.loadExtents();
    }
    return extents;
  }

  @Override
  public int count() {
    if(this.extents.size() == 0){
      this.loadExtents();
    }
    return extents.size();
  }

  @Override
  public int maximumCount() {
    int min = 0;
    for (int i = 0; i < iterators.length; i++) {
      min = Math.min(min, ((MovableCountIterator) iterators[i]).maximumCount());
    }
    return min;

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
