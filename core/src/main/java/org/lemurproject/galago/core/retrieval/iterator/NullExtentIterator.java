// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author trevor
 * @author irmarc
 */
public class NullExtentIterator extends ValueIterator implements MovableExtentIterator, MovableCountIterator {

  ExtentArray array = new ExtentArray();

  public NullExtentIterator(){
    
  }
  
  public NullExtentIterator(NodeParameters p){
    // nothing
  }
  
  public boolean nextEntry() {
    return false;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public ExtentArray extents() {
    return array;
  }

  @Override
  public int count() {
    return 0;
  }

  @Override
  public int maximumCount() {
    return 0;
  }
  
  @Override
  public void reset() {
    // do nothing
  }

  @Override
  public ExtentArray getData() {
    return array;
  }

  @Override
  public long totalEntries() {
    return 0;
  }

  @Override
  public int currentCandidate() {
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean hasMatch(int id) {
    return false;
  }

  @Override
  public String getEntry() throws IOException {
    return "NULL";
  }

  @Override
  public void moveTo(int identifier) throws IOException {
  }

  @Override
  public void movePast(int identifier) throws IOException {
  }

  @Override
  public int compareTo(MovableIterator t) {
    return 1;
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public String getKeyString() throws IOException {
    return "";
  }

  @Override
  public byte[] getKeyBytes() throws IOException {
    return new byte[0];
  }

  @Override
  public AnnotatedNode getAnnotatedNode() {
    String type = "extent";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = array.toString();
    List<AnnotatedNode> children = Collections.EMPTY_LIST;
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
