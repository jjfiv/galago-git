/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.MovableLengthsIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class InsertLengthsTraversal extends Traversal {

  private Parameters queryParams;
  private Parameters globalParams;
  private Node lenNode;
  private Retrieval retrieval;

  public InsertLengthsTraversal(Retrieval retrieval, Parameters queryParameters) {
    // TODO: devise mechanisms to use specific lengths smoothing methods
    this.retrieval = retrieval;
    this.globalParams = retrieval.getGlobalParameters();
    this.queryParams = queryParameters;

    // default lengths node.
    lenNode = new Node("lengths", new NodeParameters());
    lenNode.getNodeParameters().set("part", "lengths");
  }

  @Override
  public Node afterNode(Node node) throws Exception {
    List<Node> children = node.getInternalNodes();
    int childIdx = 0;

    if (children.isEmpty()) {
      return node;
    }

    NodeType nt = retrieval.getNodeType(node);

    Constructor cons = nt.getConstructor();
    Class[] params = cons.getParameterTypes();

    for (int idx = 0; idx < params.length; idx++) {
      if (MovableLengthsIterator.class.isAssignableFrom(params[idx])) {
        Node child = (childIdx < children.size()) ? children.get(childIdx) : null;
        NodeType cnt = (child != null) ? retrieval.getNodeType(child) : null;

        if (cnt == null || !MovableLengthsIterator.class.isAssignableFrom(cnt.getIteratorClass())) {
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
      } else if (Parameters.class.isAssignableFrom(params[idx])) {
        childIdx--;
      } else if (NodeParameters.class.isAssignableFrom(params[idx])) {
        childIdx--;
      }
      childIdx++;
    }
    return node;
  }

  @Override
  public void beforeNode(Node object) throws Exception {
    // Do nothing
  }

  /**
   * this function inserts a passage restriction to length nodes.
   */
  private Node addExtentFilters(Node in) throws Exception {
    boolean passageQuery = this.globalParams.get("passageQuery", false) || this.globalParams.get("extentQuery", false);
    passageQuery = this.queryParams.get("passageQuery", passageQuery) || this.queryParams.get("extentQuery", passageQuery);
    if (passageQuery) {
      ArrayList<Node> children = new ArrayList<Node>();
      children.add(in);
      Node replacement = new Node("passagelengths", children);
      return replacement;
    } else {
      return in;
    }
  }
}
