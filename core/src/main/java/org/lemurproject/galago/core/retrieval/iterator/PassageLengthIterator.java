/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.PassageScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author sjh
 */
public class PassageLengthIterator extends TransformIterator implements LengthsIterator {

  boolean lenCheck;
  NodeParameters np;
  LengthsIterator lengths;

  public PassageLengthIterator(NodeParameters params, LengthsIterator iterator) {
    super(iterator);
    np = params;
    lenCheck = np.get("lenCheck", true);
    lengths = iterator;
  }

  @Override
  public int length(ScoringContext c) {
    int begin, end;

    PassageScoringContext passContext = ((c instanceof PassageScoringContext) ? (PassageScoringContext) c : null);

    if (passContext != null && !lenCheck) {
      begin = passContext.begin;
      end = passContext.end;

    } else if (passContext != null && lenCheck) {
      begin = passContext.begin;
      end = Math.min(passContext.end, lengths.length(c));

    } else {
      begin = 0;
      end = lengths.length(c);
    }

    // sjh: this is possible - where the lengths are based on fields, and we are 'lenCheck'.
    if (begin >= end) {
      return 0;
//      throw new RuntimeException("PassageLengthIterator is trying to return a negative length value for document " + this.currentCandidate() + ".");
    }

    return end - begin;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "lengths";
    String className = this.getClass().getSimpleName();
    String parameters = this.np.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = Integer.toString(length(c));
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode(c));

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
