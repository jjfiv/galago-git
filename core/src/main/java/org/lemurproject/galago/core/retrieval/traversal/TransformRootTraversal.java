// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * StructuredQuery may put a "root" operator at the top of the query tree. We now have to
 * make that node have a proper operator given the processing context (i.e. parameters)
 * @author irmarc
 *
 */
public class TransformRootTraversal extends Traversal {

  int levels = 0;
  QueryType qType;
  Retrieval retrieval;
  
  public TransformRootTraversal(Retrieval r) {
    this.retrieval = r;
  }

  public void beforeNode(Node object) throws Exception {
    levels++;
  }

  public Node afterNode(Node original) throws Exception {
    levels--;
    if (levels > 0) {
      return original;
    } else {
      if (original.getNodeParameters().containsKey("queryType")) {
        String type = original.getNodeParameters().getString("queryType");
        if (type.equals("count")) {
          return transformCountRoot(original);
        } else if (type.equals("boolean")) {
          return transformBooleanRoot(original);
        } else {
          return transformRankedRoot(original);
        }
      } else if (original.getOperator().equals("root")) {
        // Not specified, and simply wrapped - have to assume ranked
        return transformRankedRoot(original);
      } else if (original.getOperator().equals("text")) {
        // Need to wrap it in a combine since we're ranking
        return transformRankedRoot(original);
      } else {
        // It's not a root node, so it's already got a query type. No more to do.
        return original;
      }
    }
  }

  private Node transformBooleanRoot(Node root) throws Exception {
    if (root.getOperator().equals("root")) {
      root.setOperator("any");
      return root;
    }

    if (retrieval.getQueryType(root) == QueryType.BOOLEAN) {
      return root;
    }

    ArrayList<Node> children = new ArrayList<Node>();
    children.add(root);
    return new Node("any", new NodeParameters(), children, root.getPosition());
  }

  private Node transformCountRoot(Node root) throws Exception {
    if (root.getOperator().equals("root")) {
      root.setOperator("synonym");
      return root;
    }

    if (retrieval.getQueryType(root) == QueryType.COUNT) {
      return root;
    }

    ArrayList<Node> children = new ArrayList<Node>();
    children.add(root);
    return new Node("synonym", new NodeParameters(), children, root.getPosition());
  }

  private Node transformRankedRoot(Node root) throws Exception {
    if (root.getOperator().equals("root")) {
      root.setOperator("combine");
      return root;
    }

    if (retrieval.getQueryType(root) == QueryType.RANKED) {
      return root;
    }

    ArrayList<Node> children = new ArrayList<Node>();
    children.add(root.clone());
    return new Node("combine", new NodeParameters(), children, root.getPosition());
  }
}
