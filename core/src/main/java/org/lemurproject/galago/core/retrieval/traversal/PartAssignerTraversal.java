/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class PartAssignerTraversal extends Traversal {

  Retrieval r;

  public PartAssignerTraversal(Retrieval r) {
    this.r = r;
  }

  @Override
  public void beforeNode(Node original, Parameters queryParameters) throws Exception {

    // if a leaf node doesn't have a part, assign one.
    if (original.numChildren() == 0 && !original.getNodeParameters().containsKey("part")) {
      TextPartAssigner.assignPart(original, queryParameters, r.getAvailableParts());
    }
  }

  @Override
  public Node afterNode(Node original, Parameters queryParameters) throws Exception {
    return original;
  }
}
