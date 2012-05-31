// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * Implements the #all indicator operator.
 * @author irmarc
 */
public class UniversalIndicatorIterator extends ConjunctionIterator implements MovableIndicatorIterator {

  public UniversalIndicatorIterator(NodeParameters p, MovableIterator[] children) {
    super(p, children);
  }

  @Override
  public boolean indicator(int identifier) {
    for(MovableIterator i : this.iterators){
      if(!i.atCandidate(identifier)){
        return false;
      }
    }
    return true;
  }

  @Override
  public String getEntry() throws IOException {
    return this.currentCandidate() + " " + this.indicator(this.currentCandidate());
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "indicator";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    int document = currentCandidate();
    boolean atCandidate = atCandidate(this.context.document);
    String returnValue = Boolean.toString(this.indicator(this.context.document));
    List<AnnotatedNode> children = new ArrayList();
    for (MovableIterator child : this.iterators) {
      children.add(child.getAnnotatedNode());
    }
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }  
}
