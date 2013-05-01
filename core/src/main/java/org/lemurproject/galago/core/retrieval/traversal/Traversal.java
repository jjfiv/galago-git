// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Basic interface for Traversals.
 * 
 * 'before's are pre-order traversals
 * 'after's are post-order traversals
 * 
 * @author trevor, sjh
 */
public abstract class Traversal {

  // root functions are called on the tree root only (must be overridden to be used)
  public void beforeTreeRoot(Node root, Parameters queryParameters) throws Exception {
  }

  public Node afterTreeRoot(Node root, Parameters queryParameters) throws Exception {
    return root;
  }

  // functions are called on every node in the tree
  public abstract void beforeNode(Node original, Parameters queryParameters) throws Exception;

  public abstract Node afterNode(Node original, Parameters queryParameters) throws Exception;
}
