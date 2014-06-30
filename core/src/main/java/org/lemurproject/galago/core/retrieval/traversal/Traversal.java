// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

/**
 * Basic interface for Traversals.
 *
 * 'before's are pre-order traversals 'after's are post-order traversals
 *
 * @author trevor, sjh
 */
public abstract class Traversal {

  /**
   * traverse is the main access point for this interface. Default behavior is
   * to perform two simultaneous traversals of the query tree: [pre-order,
   * post-order]. However, this behavior can be overridden, as required.
   *
   * The two corresponding abstract functions are called on each node in the
   * tree.
   *
   * @param tree a parsed query
   * @param queryParams
   * @return query tree
   * @throws Exception
   */
  public Node traverse(Node tree, Parameters queryParams) throws Exception {
    beforeNode(tree, queryParams);
    for (int i = 0; i < tree.numChildren(); i++) {
      tree.replaceChildAt(traverse(tree.getChild(i), queryParams), i);
    }
    return afterNode(tree, queryParams);
  }

  // functions are called on every node in the tree
  public abstract void beforeNode(Node original, Parameters queryParameters) throws Exception;

  public abstract Node afterNode(Node original, Parameters queryParameters) throws Exception;
}
