// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * <p>Node represents a single node in a query parse tree.</p>
 * 
 * <p>In Galago, queries are parsed into a tree of Nodes.  The query tree can then
 * be modified using StructuredQuery.copy, or analyzed by using StructuredQuery.walk.
 * Once the query is in the proper form, the query is converted into a tree of iterators
 * that can be evaluated.</p>
 *
 * @author trevor
 */
public class Node implements Serializable {
  /// The query operator represented by this node, like "combine", "weight", "syn", etc.

  private String operator;
  /// Child nodes of the operator, e.g. in #combine(a b), 'a' and 'b' are internal nodes of #combine.
  private List<Node> internalNodes;
  // Parent node - null if it is root
  private Node parent;
  // The position in the text string where this operator starts.  Useful for parse error messages.
  private int position;
  /// Additional nodeParameters for this operator; usually these are term statistics and smoothing nodeParameters.
  private NodeParameters nodeParameters;

  public Node() {
    operator = "";
    internalNodes = new ArrayList<Node>();
    nodeParameters = new NodeParameters();
    position = 0;
    parent = null;
  }

  public Node(String operator, List<Node> internalNodes) {
    this(operator, new NodeParameters(), internalNodes, 0);
  }

  public Node(String operator, List<Node> internalNodes, int position) {
    this(operator, new NodeParameters(), internalNodes, position);
  }

  public Node(String operator, String argument) {
    this(operator, new NodeParameters(argument), new ArrayList<Node>(), 0);
  }

  public Node(String operator, String argument, int position) {
    this(operator, new NodeParameters(argument), new ArrayList<Node>(), position);
  }

  public Node(String operator, String argument, List<Node> internalNodes) {
    this(operator, new NodeParameters(argument), internalNodes, 0);
  }

  public Node(String operator, NodeParameters np) {
    this(operator, np, new ArrayList(), 0);
  }

  public Node(String operator, NodeParameters np, List<Node> internalNodes) {
    this(operator, np, internalNodes, 0);
  }

  public Node(String operator, String argument, List<Node> internalNodes, int position) {
    this(operator, new NodeParameters(argument), internalNodes, position);
  }

  public Node(String operator, NodeParameters nodeParameters, List<Node> internalNodes, int position) {
    this.operator = operator;
    this.internalNodes = internalNodes;
    this.position = position;
    this.nodeParameters = nodeParameters;
    this.parent = null;

    for (Node c : internalNodes) {
      c.setParent(this);
    }
  }

  public void clearParent() {
    this.parent = null;
  }

  public Node clone() {
    ArrayList newInternals = new ArrayList();
    for (Node n : this.internalNodes) {
      newInternals.add(n.clone());
    }
    return new Node(operator, nodeParameters.clone(), newInternals, position);
  }

  public String getDefaultParameter() {
    return nodeParameters.get("default", null);
  }

  public String getOperator() {
    return operator;
  }

  public List<Node> getInternalNodes() {
    return Collections.unmodifiableList(this.internalNodes);
  }

  public int getPosition() {
    return position;
  }

  public NodeParameters getNodeParameters() {
    return nodeParameters;
  }

  public Node getParent() {
    return parent;
  }

  public void setParent(Node parent) {
    assert (this.parent == null) : "Nodes may only have one parent.\n"
            + this.toString() + " points to: " + parent.toString() + "\n"
            + "Use clearParent() to remove the previous parent.\n";
    this.parent = parent;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append('#');
    assert !operator.contains(":") && !operator.contains("(") : "Operator can not contain ':' or '('.";
    builder.append(operator);

    builder.append(nodeParameters.toString());

    if (internalNodes.size() == 0) {
      builder.append("()");
    } else {
      builder.append("( ");
      for (Node child : internalNodes) {
        builder.append(child.toString());
        builder.append(' ');
      }
      builder.append(")");
    }

    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Node)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    Node other = (Node) o;

    if ((operator == null) != (other.getOperator() == null)) {
      return false;
    }
    if (operator != null && !other.getOperator().equals(operator)) {
      return false;
    }

    String thisDefault = this.nodeParameters.getAsString("default");
    String thatDefault = other.nodeParameters.getAsString("default");

    if ((thisDefault == null && thatDefault != null)
            || (thisDefault != null && thatDefault == null)) {
      return false;
    }
    if (thisDefault != null && !thisDefault.equals(thatDefault)) {
      return false;
    }

    if (internalNodes.size() != other.getInternalNodes().size()) {
      return false;
    }
    for (int i = 0; i < internalNodes.size(); i++) {
      if (!internalNodes.get(i).equals(other.getInternalNodes().get(i))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 67 * hash + (this.operator != null ? this.operator.hashCode() : 0);
    hash = 67 * hash + (this.internalNodes != null ? this.internalNodes.hashCode() : 0);
    return hash;
  }

  public static List<Node> cloneNodeList(List<Node> textNodes) {
    ArrayList<Node> newNodes = new ArrayList();
    for (Node n : textNodes) {
      newNodes.add(n.clone());
    }
    return newNodes;
  }
}
