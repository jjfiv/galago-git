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
public abstract class ExtentDisjunctionIterator extends DisjunctionIterator implements MovableDataIterator<ExtentArray>, MovableExtentIterator, CountIterator {

  protected ExtentArray extentCache;
  protected byte[] key;

  public ExtentDisjunctionIterator(MovableExtentIterator[] iterators) throws IOException {
    super(iterators);
    this.extentCache = new ExtentArray();
    buildKey(iterators);
  }

  @Override
  public boolean hasMatch(int identifier) {
    return super.hasMatch(identifier) && extents().size() > 0;
  }

  @Override
  public String getEntry() throws IOException {
    ArrayList<String> strs = new ArrayList<String>();
    ExtentArrayIterator eai = new ExtentArrayIterator(extents());
    while (!eai.isDone()) {
      strs.add(String.format("[%d, %d]", eai.currentBegin(), eai.currentEnd()));
      eai.next();
    }
    return Utility.join(strs.toArray(new String[0]), ",");
  }

  @Override
  public ExtentArray extents() {
    this.loadExtents();
    return extentCache;
  }

  @Override
  public ExtentArray getData() {
    return extents();
  }

  @Override
  public int count() {
    return extents().size();
  }

  @Override
  public int maximumCount() {
    int sum = 0;
    for (int i = 0; i < iterators.length; i++) {
      sum += ((CountIterator) iterators[i]).maximumCount();
    }
    return sum;

  }

  @Override
  public byte[] key() {
    return key;
  }

  protected void buildKey(MovableExtentIterator[] iterators) {
    int keysize = 2;
    for (int i = 0; i < iterators.length; i++) {
      keysize += iterators[i].key().length;
    }
    key = new byte[keysize];
    keysize = 2;
    key[0] = 'D' >> 8; // conjunction marker;                                                                                               
    key[1] = 'D' & 0xFF;
    for (int i = 0; i < iterators.length; i++) {
      MovableExtentIterator it = iterators[i];
      byte[] inner = it.key();
      System.arraycopy(inner, 0, key, keysize, inner.length);
      keysize += inner.length;
    }
  }

  public abstract void loadExtents();

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    this.loadExtents();
    String type = "extent";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = extents().toString();
    List<AnnotatedNode> children = new ArrayList();
    for (MovableIterator child : this.iterators) {
      children.add(child.getAnnotatedNode());
    }
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
