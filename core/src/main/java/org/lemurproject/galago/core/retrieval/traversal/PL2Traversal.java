/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class PL2Traversal extends Traversal {
  
  private final Retrieval ret;
  private final Parameters queryParams;
  
  public PL2Traversal(Retrieval ret, Parameters queryParams) {
    this.ret = ret;
    this.queryParams = queryParams;
  }
  
  @Override
  public Node afterNode(Node original) throws Exception {
    if (original.getOperator().equals("pl2")) {
      Node pl2root = new Node("combine");
      pl2root.getNodeParameters().set("norm", false);
      for (int i = 0; i < original.numChildren(); i++) {
        Node scorer = new Node("pl2scorer");
        scorer.addChild(StructuredQuery.parse("#lengths:document:part=lengths()"));
        scorer.addChild(original.getChild(i));
        pl2root.addChild(scorer);
      }
      return pl2root;
    }
    return original;
  }
  
  @Override
  public void beforeNode(Node object) throws Exception {
    // do nothing
  }
}
