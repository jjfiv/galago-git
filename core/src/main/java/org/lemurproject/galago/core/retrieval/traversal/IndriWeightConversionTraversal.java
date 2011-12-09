// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author trevor
 */
public class IndriWeightConversionTraversal implements Traversal {

  public IndriWeightConversionTraversal(Retrieval retrieval) {
  }

  public void beforeNode(Node object) throws Exception {
    // do nothing
  }

  public double getWeight(Node weightNode) {
    // if weight value is: xx.yy
    if (weightNode.getOperator().equals("inside")) {
      if (weightNode.getInternalNodes().size() != 2) {
        return 1.0;
      } else {
        Node inner = weightNode.getInternalNodes().get(0);
        Node outer = weightNode.getInternalNodes().get(1);
        return Double.parseDouble(inner.getDefaultParameter() + "." + outer.getDefaultParameter());
      }
    } else {
      return Double.parseDouble(weightNode.getDefaultParameter());
    }
  }

  public Node afterNode(Node node) throws Exception {
    if (node.getOperator().equals("weight")) {
      List<Node> children = node.getInternalNodes();

      // first, verify that the appropriate children are weights
      if (children.size() % 2 == 1) {
        throw new IOException("A weighted node cannot have an odd number of internal nodes: "
                + node.getInternalNodes().size());
      }

      // now, reassemble everything:
      NodeParameters newParameters = node.getNodeParameters();
      ArrayList<Node> newChildren = new ArrayList<Node>();
      for (int i = 0; i < children.size(); i += 2) {
        Node weightNode = children.get(i);
        Node childNode = children.get(i + 1);

        newChildren.add(childNode);
        newParameters.set(Integer.toString(i / 2), getWeight(weightNode));
      }

      return new Node("combine", newParameters, Node.cloneNodeList(newChildren), node.getPosition());
    } else {
      return node;
    }
  }
}
