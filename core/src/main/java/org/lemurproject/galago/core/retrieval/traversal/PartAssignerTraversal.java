/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.utility.Parameters;

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
    // only assing parts to leaf nodes
    if (original.numChildren() > 0)
      return;

    // skip anything that's not a "text" node
    if(!original.isText())
      return;

    // don't need to reassign anything
    if(original.getNodeParameters().containsKey("part"))
      return;

    TextPartAssigner.assignPart(original, queryParameters, r.getAvailableParts());
  }

  @Override
  public Node afterNode(Node original, Parameters queryParameters) throws Exception {
    return original;
  }
}
