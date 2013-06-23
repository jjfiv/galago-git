package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 * Wraps the WindowIndexReader to act as a lengths reader for a particular
 * field.
 *
 * @author irmarc
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
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "lengths";
    String className = this.getClass().getSimpleName();
    String parameters = this.np.toString();
    int document = currentCandidate();
    boolean hasMatch = hasMatch(this.context.document);
    String returnValue = Integer.toString(this.length());
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode());

    return new AnnotatedNode(type, className, parameters, document, hasMatch, returnValue, children);
  }

  @Override
  public int length() {
    ExtentArray ar = extItr.extents();
    int len = 0;
    // IGNORING OVERLAPPING EXTENTS //
    for (int i = 0; i < ar.size(); i++) {
      len += ar.end(i) - ar.begin(i);
    }
    return len;
  }
}