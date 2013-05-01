/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal.optimize;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * This traversal removes extraneous combines.
 * #combine( #combine( a b ) #combine( c d f ) #combine( e ) )  ->
 * #combine( a b c d f e )
 *
 * If there are weights on each of the components, they are rescaled.
 * 
 * @author sjh
 */
public class FlattenCombineTraversal extends Traversal {

  boolean flattenDefault;

  public FlattenCombineTraversal(Retrieval ret, Parameters queryParameters) {
    Parameters globalParams = ret.getGlobalParameters();

    flattenDefault = globalParams.get("flattenCombine", true);
  }

  @Override
  public void beforeNode(Node object, Parameters qp) throws Exception {
  }

  @Override
  public Node afterNode(Node original, Parameters qp) throws Exception {

    boolean flatten = qp.get("flattenCombine", flattenDefault);
    
    if (flatten) {
      // flatten combine nodes
      if (original.getOperator().equals("combine")) {
        List<Node> children = original.getInternalNodes();
        NodeParameters oldParameters = original.getNodeParameters();

        boolean nestedCombine = false;
        ArrayList<Node> newChildren = new ArrayList();
        NodeParameters newParameters = new NodeParameters();

        assert(children.size() > 0): "#combine operators must have more than one child.";

        for (int i = 0; i < children.size(); i++) {
          Node child = children.get(i);
          // if we have a nested combine - collect sub children
          if (child.getOperator().equals("combine")) {
            nestedCombine = true;
            List<Node> subChildren = child.getInternalNodes();
            double weightSum = 0.0;
            for (int j = 0; j < subChildren.size(); j++) {
              weightSum += child.getNodeParameters().get(Integer.toString(j), 1.0);
            }
            for (int j = 0; j < subChildren.size(); j++) {
              Node subChild = subChildren.get(j);
              double normWeight = child.getNodeParameters().get(Integer.toString(j), 1.0) / weightSum;
              double newWeight = oldParameters.get(Integer.toString(i), 1.0) * normWeight;
              newParameters.set(Integer.toString(newChildren.size()), newWeight);
              newChildren.add(subChild);
            }

          } else {
            // otherwise we have a normal child
            // newChildren.size == the new index of this child
            newParameters.set(Integer.toString(newChildren.size()), oldParameters.get(Integer.toString(i), 1.0));
            newChildren.add(child);
          }
        }

        if (nestedCombine) {
          // TODO: Check node tying
          return new Node("combine", newParameters, Node.cloneNodeList(newChildren), original.getPosition());
        }
      }
    }

    return original;
  }
}
