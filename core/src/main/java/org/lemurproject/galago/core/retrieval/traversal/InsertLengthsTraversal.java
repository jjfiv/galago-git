/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
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
    NodeType nt = retrieval.getNodeType(node);
    if (ScoringFunctionIterator.class.isAssignableFrom(nt.getIteratorClass())) {
      // if we have only one child - (we assume it is a count node)
      if (node.numChildren() == 1) {

        // default lengths node:
        Node lenNodeClone = lenNode.clone();
        // check if there is a specific field to smooth with
        String field = node.getNodeParameters().get("lengths", "document");
        lenNodeClone.getNodeParameters().set("default", field);

        // add passage length wrapper
        lenNodeClone = addExtentFilters(lenNodeClone);

        // add this node at position 0.
        node.addChild(lenNodeClone, 0);
      }
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
    boolean passageQuery = this.globalParams.get("passageQuery", false);
    passageQuery = this.queryParams.get("passageQuery", passageQuery);
    if (passageQuery){
      ArrayList<Node> children = new ArrayList<Node>();
      children.add(in);
      Node replacement = new Node("passagelengths", children);
      return replacement;
    } else {
      return in;
    }
  }
}
