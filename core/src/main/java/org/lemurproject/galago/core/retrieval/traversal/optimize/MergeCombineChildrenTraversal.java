/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal.optimize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.utility.Parameters;

/**
 * This traversal removes extraneous operators:
 * 
 *  #<windowop>:<size>( X ) 
 *    --> X
 * 
 * @author sjh
 */
public class MergeCombineChildrenTraversal extends Traversal {

  public MergeCombineChildrenTraversal() {
  }

  @Override
  public void beforeNode(Node object, Parameters qp) throws Exception {
  }

  @Override
  public Node afterNode(Node original, Parameters qp) throws Exception {

    // merge combine children nodes and add weights together
    if (original.getOperator().equals("combine")) {
      List<Node> children = original.getInternalNodes();
      NodeParameters oldParameters = original.getNodeParameters();
      HashMap<String, Double> mergedWeights = new HashMap();

      for (int i = 0; i < children.size(); i++) {
        String cString = children.get(i).toString();
        if (!mergedWeights.containsKey(cString)) {
          mergedWeights.put(cString, oldParameters.get(Integer.toString(i), 1D));
        } else {
          mergedWeights.put(cString, mergedWeights.get(cString)
                  + oldParameters.get(Integer.toString(i), 1D));
        }
      }

      if (mergedWeights.size() < children.size()) {
        ArrayList<Node> newChildren = new ArrayList();
        NodeParameters newParameters = new NodeParameters();
        for (Node n : children) {
          String nStr = n.toString();
          if (mergedWeights.containsKey(nStr)) {
            newParameters.set(Integer.toString(newChildren.size()), mergedWeights.get(nStr));
            newChildren.add(n);
            mergedWeights.remove(nStr);
          }
        }
        // TODO: Check node tying
        Node reducedCombine = new Node("combine", newParameters, Node.cloneNodeList(newChildren), original.getPosition());
        return reducedCombine;
      }
    }

    return original;
  }
}
