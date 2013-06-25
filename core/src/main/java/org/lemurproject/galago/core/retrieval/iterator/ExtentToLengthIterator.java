package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 * Wraps an extent iterator to act as a lengths iterator ** overlapping extents
 * are NOT detected NOR avoided **
 *
 * @author sjh
 */
public class ExtentToLengthIterator extends TransformIterator implements LengthsIterator {

  private NodeParameters np;
  private ExtentIterator extItr;

  public ExtentToLengthIterator(NodeParameters p, ExtentIterator itr) {
    super(itr);
    this.np = p;
    this.extItr = itr;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "lengths";
    String className = this.getClass().getSimpleName();
    String parameters = this.np.toString();
    long document = currentCandidate();
    boolean hasMatch = hasMatch(c.document);
    String returnValue = Integer.toString(this.length(c));
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode(c));

    return new AnnotatedNode(type, className, parameters, document, hasMatch, returnValue, children);
  }

  @Override
  public int length(ScoringContext c) {
    ExtentArray ar = extItr.extents(c);
    int len = 0;
    // IGNORING OVERLAPPING EXTENTS //
    for (int i = 0; i < ar.size(); i++) {
      len += ar.end(i) - ar.begin(i);
    }
    return len;
  }
}