/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.utility.Parameters;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 *
 * @author sjh
 */
public class InsertLengthsTraversal extends Traversal {

  private Node lenNode;
  private Retrieval retrieval;

  public InsertLengthsTraversal(Retrieval retrieval) {
    this.retrieval = retrieval;

    // default lengths node.
    lenNode = new Node("lengths", new NodeParameters());
    lenNode.getNodeParameters().set("part", "lengths");
  }

  @Override
  public Node afterNode(Node node, Parameters qp) throws Exception {
    List<Node> children = node.getInternalNodes();
    int childIdx = 0;

    if (children.isEmpty()) {
      return node;
    }

    NodeType nt = retrieval.getNodeType(node);
    
    // if we're doing something special -- don't insert a length.
    if(nt == null){
      return node;
    }
    
    Constructor cons = nt.getConstructor();
    Class<?>[] params = cons.getParameterTypes();

    for (Class<?> param : params) {
      if (LengthsIterator.class.isAssignableFrom(param)) {
        Node child = (childIdx < children.size()) ? children.get(childIdx) : null;
        NodeType cnt = (child != null) ? retrieval.getNodeType(child) : null;

        if (cnt == null || !LengthsIterator.class.isAssignableFrom(cnt.getIteratorClass())) {
          // then we need a lengths iterator here.
          // default lengths node:
          Node lenNodeClone = lenNode.clone();
          // check if there is a specific field to smooth with
          String field = node.getNodeParameters().get("lengths", "document");
          lenNodeClone.getNodeParameters().set("default", field);

          // add passage length wrapper
//          lenNodeClone = addExtentFilters(lenNodeClone);

          // add this node at position 0.
          node.addChild(lenNodeClone, childIdx);
          childIdx++;
        }
      } else if (Parameters.class.isAssignableFrom(param)) {
        childIdx--;
      } else if (NodeParameters.class.isAssignableFrom(param)) {
        childIdx--;
      }
      childIdx++;
    }
    return node;
  }

  @Override
  public void beforeNode(Node object, Parameters qp) throws Exception {
    // Do nothing
  }
}
