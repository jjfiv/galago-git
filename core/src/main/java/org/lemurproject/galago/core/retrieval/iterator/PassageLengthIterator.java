/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.processing.PassageScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class PassageLengthIterator extends TransformIterator implements LengthsIterator {

  boolean lenCheck;
  NodeParameters np;
  PassageScoringContext passContext;
  LengthsIterator lengths;

  public PassageLengthIterator(NodeParameters params, LengthsIterator iterator) {
    super(iterator);
    np = params;
    lenCheck = np.get("lenCheck", true);
    lengths = iterator;

    passContext = null;
  }

  @Override
  public void setContext(ScoringContext context){
    super.setContext(context);
    if(context instanceof PassageScoringContext){
      passContext = (PassageScoringContext) context;
    }
  }

  @Override
  public int length() {
    int begin, end;

    if (passContext != null && !lenCheck) {
      begin = passContext.begin;
      end = passContext.end;

    } else if (passContext != null && lenCheck) {
      begin = passContext.begin;
      end = Math.min(passContext.end, lengths.length());

    } else {
      begin = 0;
      end = lengths.length();
    }

    // sjh: this is possible - where the lengths are based on fields, and we are 'lenCheck'.
    if (begin >= end) {
      return 0;
//      throw new RuntimeException("PassageLengthIterator is trying to return a negative length value for document " + this.currentCandidate() + ".");
    }

    return end - begin;
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "lengths";
    String className = this.getClass().getSimpleName();
    String parameters = this.np.toString();
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Integer.toString(this.length());
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode());

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
