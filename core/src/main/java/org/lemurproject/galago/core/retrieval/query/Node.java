// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.query;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * <p>Node represents a single node in a query parse tree.</p>
 *
 * <p>In Galago, queries are parsed into a tree of Nodes. The query tree can
 * then be modified using StructuredQuery.copy, or analyzed by using
 * StructuredQuery.walk. Once the query is in the proper form, the query is
 * converted into a tree of iterators that can be evaluated.</p>
 *
 * @author trevor, sjh
 */
public class Node implements Serializable {

  private static final Set<String> defaultOmissionSet;

  static {
    defaultOmissionSet = new HashSet<String>();
    defaultOmissionSet.add("lengths");
    defaultOmissionSet.add("passagelengths");
    defaultOmissionSet.add("passagefilter");
    defaultOmissionSet.add("part");
  }
  
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
  private static final long serialVersionUID = 4553653651892088433L;

  public Node() {
    this("", new NodeParameters(), new ArrayList<Node>(), 0);
  }

  public Node(String operator) {
    this(operator, new NodeParameters(), new ArrayList<Node>(), 0);
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
    this(operator, np, new ArrayList<Node>(), 0);
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

    this.internalNodes = new ArrayList<>();
    for (Node c : internalNodes) {
      addChild(c);
    }
  }

  /**
   * Deep-clones this Node. Be aware this clones the *entire* subtree rooted at
   * this node, therefore all descendants are also cloned.
   *
   */
  @Override
  public Node clone() {
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
      assert (child.parent == this);
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
    assert (child.parent == this);
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
  
  public String toSimplePrettyString() {
    return toSimplePrettyString("", defaultOmissionSet, "");
  }

  public String toSimplePrettyString(Set<String> ignoreParams) {
    return toSimplePrettyString("", ignoreParams, "");
  }

  public String toSimplePrettyString(String indent, Set<String> ignoreParams) {
    return toSimplePrettyString(indent, ignoreParams, "");
  }

  public String toSimplePrettyString(String indent, Set<String> ignoreParams, String addOnString) {
    StringBuilder builder = new StringBuilder();

    if (ignoreParams.contains(operator)) {
      if (internalNodes.size() != 0) {
        for (Node child : internalNodes) {
          String childString = child.toSimplePrettyString(indent, ignoreParams, addOnString);
          if (!childString.replaceAll("\\s", "").equals("")) {
            builder.append(childString);
          }
        }
      }
    } else {
      if (operator.equals("combine")) {
        builder.append(indent);
        builder.append(addOnString);
        builder.append('#');
        assert !operator.contains(":") && !operator.contains("(") : "Operator can not contain ':' or '('.";
        builder.append(operator);
        ArrayList<String> combineWeightList = nodeParameters.collectCombineWeightList();
        if (combineWeightList.size() == 0 && internalNodes.size() > 0) {
          DecimalFormat formatter = new DecimalFormat("###.#####");
          String weightString = formatter.format(1.0 / internalNodes.size());
          int firstNonZeroIndex = -1;
          for (int i = 0; i < weightString.length(); i++) {
            if (weightString.charAt(i) != '0' && weightString.charAt(i) != '.') {
              firstNonZeroIndex = i;
              break;
            }
          }
          if (firstNonZeroIndex != -1) {
            if (weightString.length() >= 5) {
              firstNonZeroIndex = firstNonZeroIndex >= 4 ? firstNonZeroIndex : 4;
              weightString = weightString.substring(0, firstNonZeroIndex + 1);
            }
          }
          for (Node internalNode : internalNodes) {
            combineWeightList.add(weightString);
          }
        }
        //builder.append(nodeParameters.toSimpleString(ignoreParams, operator));
        builder.append("(\n");
        for (int i = 0; i < internalNodes.size(); i++) {
          Node child = internalNodes.get(i);
          String childString = child.toSimplePrettyString(indent + "    ", ignoreParams, "");
          if (!childString.replaceAll("\\s", "").equals("")) {
            if (i < combineWeightList.size()) {
              builder.append(child.toSimplePrettyString(indent + "    ", ignoreParams, combineWeightList.get(i) + "\t"));
            } else {
              builder.append(child.toSimplePrettyString(indent + "    ", ignoreParams, ""));
            }
            builder.append("\n");
          }
        }
        builder.append(indent).append(")");
      } else if (operator.equals("unordered") || operator.equals("ordered") || operator.equals("syn")) {
        builder.append(indent);
        builder.append(addOnString);
        builder.append('#');
        assert !operator.contains(":") && !operator.contains("(") : "Operator can not contain ':' or '('.";
        builder.append(operator);
        builder.append(nodeParameters.toSimpleString(ignoreParams, operator));
        builder.append("(");
        for (Node child : internalNodes) {
          String childString = child.toSimplePrettyString("", ignoreParams, "");
          if (!childString.replaceAll("\\s", "").equals("")) {
            builder.append(childString).append(" ");
          }
        }
        builder.append(")");
      } else if (operator.equals("extents") || operator.equals("counts")) {
        builder.append(indent);
        builder.append(addOnString);
        builder.append(nodeParameters.toSimpleString(ignoreParams, operator));
        for (Node child : internalNodes) {
          String childString = child.toSimplePrettyString(indent + "    ", ignoreParams, "");
          if (!childString.replaceAll("\\s", "").equals("")) {
            builder.append(childString);
          }
        }
      } else {
        builder.append(indent);
        builder.append(addOnString);
        builder.append('#');
        assert !operator.contains(":") && !operator.contains("(") : "Operator can not contain ':' or '('.";
        builder.append(operator);
        builder.append(nodeParameters.toSimpleString(ignoreParams, operator));
        if (internalNodes.size() > 0) {
          builder.append("(");
          for (Node child : internalNodes) {
            String childString = child.toSimplePrettyString(indent + "    ", ignoreParams, "");
            if (!childString.replaceAll("\\s", "").equals("")) {
              builder.append(childString);
            }
          }
          builder.append(indent).append("    ").append(")");
        }
      }
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

  public static List<Node> cloneNodeList(List<Node> nodeList) {
    ArrayList<Node> newNodes = new ArrayList<Node>();
    for (Node n : nodeList) {
      newNodes.add(n.clone());
    }
    return newNodes;
  }

  /** Build a text node at the first position */
  public static Node Text(String text) {
    return Text(text, 0);
  }
  /** Build a text node at the given position */
  public static Node Text(String text, int position) {
    return new Node("text", new NodeParameters(text), new ArrayList<Node>(), position);
  }

  public boolean isText() {
    return operator.equals("extents") || operator.equals("counts") || operator.equals("text");
  }
}

