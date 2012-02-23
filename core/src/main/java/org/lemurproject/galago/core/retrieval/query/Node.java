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
    this.position = position;
    this.nodeParameters = nodeParameters;
    this.parent = null;

    this.internalNodes = new ArrayList<Node>();
    for (Node c : internalNodes) {
      addChild(c);
    }
  }

  /**
   * Deep-clones this Node. Be aware this clones the *entire* subtree rooted at this node,
   * therefore all descendants are also cloned.
   *
   * @return
   */
  @Override
  public Node clone() {
    ArrayList newInternals = new ArrayList();
    return new Node(operator, nodeParameters.clone(), cloneNodeList(this.internalNodes), position);
  }

  public String getDefaultParameter() {
    return nodeParameters.get("default", null);
  }

  public String getOperator() {
    return operator;
  }

  public void setOperator(String op) {
    this.operator = op;
  }

  public void clearChildren() {
    internalNodes.clear();
  }

  public void removeChildAt(int i) {
    Node child = internalNodes.remove(i);
    if (child != null) {
      assert(child.parent == this);
      child.parent = null;
    }
  }

  public void replaceChildAt(Node newChild, int i) {
    assert (i > -1 && i < internalNodes.size());
    newChild.parent = this;
    Node oldChild = internalNodes.set(i, newChild);
    if (oldChild != newChild) {
      oldChild.parent = null;
    }
  }

  public void removeChild(Node child) {
    assert(child.parent == this);
    child.parent = null;
    internalNodes.remove(child);
  }

  public void addChild(Node child) {
    this.addChild(child, -1);
  }

  public void addChild(Node child, int pos) {
    // link to this parent
    child.parent = this;
    if (pos < internalNodes.size() && pos > -1) {
      internalNodes.add(pos, child);
    } else {
      internalNodes.add(child);
    }
  }

  public Node getChild(int index) {
    return internalNodes.get(index);
  }

  public int numChildren() {
    return this.internalNodes.size();
  }

  public Iterator<Node> getChildIterator() {
    return internalNodes.iterator();
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

  public String toPrettyString() {
    return toPrettyString("");
  }

  public String toPrettyString(String indent) {
    StringBuilder builder = new StringBuilder();
    
    builder.append(indent);

    builder.append('#');
    assert !operator.contains(":") && !operator.contains("(") : "Operator can not contain ':' or '('.";
    builder.append(operator);

    builder.append(nodeParameters.toString());

    if (internalNodes.size() == 0) {
      builder.append("()\n");
    } else {
      builder.append("(\n");
      for (Node child : internalNodes) {
        builder.append(child.toPrettyString(indent + "    "));
      }
      builder.append(indent).append("  ").append(")\n");
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
