/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class ExtentConjunctionIterator extends ConjunctionIterator implements DataIterator<ExtentArray>, ExtentIterator, CountIterator {

  protected ExtentArray extentCache;
  protected byte[] key;

  public ExtentConjunctionIterator(NodeParameters parameters, ExtentIterator[] iterators) throws IOException {
    super(parameters, iterators);
    this.extentCache = new ExtentArray();
  }

  @Override
  public String getValueString() throws IOException {
    ArrayList<String> strs = new ArrayList<String>();
    ExtentArrayIterator eai = new ExtentArrayIterator(extents(context));
    while (!eai.isDone()) {
      strs.add(String.format("[%d, %d]", eai.currentBegin(), eai.currentEnd()));
      eai.next();
    }
    return Utility.join(strs.toArray(new String[0]), ",");
  }

  @Override
  public ExtentArray extents(ScoringContext c) {
    this.loadExtents(c);
    return extentCache;
  }

  @Override
  public ExtentArray data(ScoringContext c) {
    return extents(c);
  }

  @Override
  public int count(ScoringContext c) {
    return extents(c).size();
  }

  public abstract void loadExtents(ScoringContext c);

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    // ensure extentCache are loaded
    this.loadExtents(c);

    String type = "extent";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = extents(c).toString();
    List<AnnotatedNode> children = new ArrayList();
    for (BaseIterator child : this.iterators) {
      children.add(child.getAnnotatedNode(c));
    }
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
