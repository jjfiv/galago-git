/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal.optimize;

import java.lang.reflect.Constructor;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.MovableExtentIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * If an extent node is a leaf node, and it's parent requires only count data, 
 *   replace the extent node with a count node.
 * 
 * @author sjh
 */
public class ExtentsToCountLeafTraversal extends Traversal {
  private final Retrieval retrieval;
  private final Parameters queryParameters;
  private final boolean flag;

  public ExtentsToCountLeafTraversal(Retrieval retrieval, Parameters queryParameters){
    this.retrieval = retrieval;
    this.queryParameters = queryParameters;
    
    this.flag = queryParameters.get("countreplace", this.retrieval.getGlobalParameters().get("countreplace", true));
  }  
  
  @Override
  public Node afterNode(Node node) throws Exception {
    /** Check if the child is an leaf 'extents' node && the parent requires only count data
     *    If so - we can replace extents with counts.
     *    This can lead to performance improvements within positions indexes
     *    as the positional data does NOT need to be read for the feature scorer to operate.
     */
    if (flag && node.getOperator().equals("extents") && node.numChildren() == 0) {
      Node parent = node.getParent();
      NodeType nt = (parent != null)? retrieval.getNodeType(parent): null;

      if(nt == null){
        return node;
      }
      
      Constructor cons = nt.getConstructor();
      Class[] params = cons.getParameterTypes();

      boolean requiresExtents = false;
      for (int idx = 0; idx < params.length; idx++) {
        requiresExtents |= MovableExtentIterator.class.isAssignableFrom(params[idx]);
        requiresExtents |= MovableExtentIterator[].class.isAssignableFrom(params[idx]);
      }

      if (!requiresExtents) {
        node.setOperator("counts");
      }
    }
    
    return node;
  }

  @Override
  public void beforeNode(Node object) throws Exception {
  }
}
