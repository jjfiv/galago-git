/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal.optimize;

import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * This traversal removes extraneous window operators:
 * 
 *  #<windowop>:<size>( X ) 
 *    --> X
 * 
 * @author sjh
 */
public class FlattenWindowTraversal extends Traversal {

  public FlattenWindowTraversal() {
  }

  @Override
  public void beforeNode(Node object) throws Exception {
  }

  @Override
  public Node afterNode(Node original) throws Exception {

    // if we have a window operator with a single child
    if (original.getOperator().equals("ordered")
            || original.getOperator().equals("syn")
            || original.getOperator().equals("synonym")
            || original.getOperator().equals("od")
            || original.getOperator().equals("unordered")
            || original.getOperator().equals("uw")) {
      List<Node> children = original.getInternalNodes();
      if (children.size() == 1) {
        return children.get(0);
      }
    }
    return original;
  }
}
