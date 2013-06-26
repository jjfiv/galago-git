// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.FilteredIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.FieldComparisonIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * For many kinds of queries, it may be preferable to not have to type
 * an explicit feature operator around a count or extents term.  For example,
 * we want #combine(#dirichlet(#counts:dog())) to be the same as
 * #combine(dog).  This transformation automatically adds the #dirichlet
 * operator.
 * 
 * (7/17/2011, irmarc): Added a check for the intersection operator. If found, makes sure to add
 *                        a -1 as the default distance, which indicates "whole doc".
 *
 * (3/2013, sjh) Added check for expected node type to restrict the conversion of extent nodes to count nodes
 * 
 * @author trevor, irmarc, sjh
 */
public class ImplicitFeatureCastTraversal extends Traversal {

  Parameters globals;
  Retrieval retrieval;

  public ImplicitFeatureCastTraversal(Retrieval retrieval) {
    this.retrieval = retrieval;
    this.globals = retrieval.getGlobalParameters();
  }

  // Put node modification in "before", since we're not replacing the node
  @Override
  public void beforeNode(Node node, Parameters queryParameters) throws Exception {
    // Indicates we want "whole doc" matching
    if (node.getOperator().equals("intersect")) {
      node.getNodeParameters().set("default", -1);
      return;
    }
  }

  @Override
  public Node afterNode(Node node, Parameters queryParameters) throws Exception {
    NodeType nt = retrieval.getNodeType(node);

    // can't do anything reliably. Return.
    if (nt == null) {
      return node;
    }

    // This moves the interior nodes of a field comparison operator into its
    // globals, which is the appropriate syntax.
    // Example:
    //
    // #lessThan( date #counts:6/16/1980:part=postings() ) -->
    //
    // #lessThan:0=6/16/1980( date )
    if (FieldComparisonIterator.class.isAssignableFrom(nt.getIteratorClass())) {
      ArrayList<Node> children = new ArrayList(node.getInternalNodes());
      if (!children.get(0).getOperator().equals("field")) {
        Node fieldNode = new Node("field", children.get(0).getDefaultParameter());
        children.remove(0);
        children.add(0, fieldNode);
      }
      int pos = 0;
      NodeParameters p = node.getNodeParameters();
      while (children.size() > 1) {
        p.set(Integer.toString(pos), children.get(1).getDefaultParameter());
        pos++;
        children.remove(1);
      }
      return new Node(node.getOperator(), p, children, node.getPosition());
    }

    // Fixes handling double-quotes (I hope).
    if (node.getOperator().equals("quote")) {
      node.getNodeParameters().set("default", "1");
      node.setOperator("od");
      return node;
    }

    // Determine if we need to add a scoring node
    Node scored = addScorers(node, queryParameters);
    return scored;
  }

  private Node addScorers(Node node, Parameters queryParameters) throws Exception {

    NodeType nodeType = retrieval.getNodeType(node);
    if (nodeType == null) {
      return node;
    }

    List<Node> children = node.getInternalNodes();
    // Given that we're going to pass children.size() children to
    // this constructor, what types should those children have?
    Class[] types = nodeType.getParameterTypes(children.size());
    if (types == null) {
      return node;
    }

    List<Node> newChildren = new ArrayList<Node>();
    for (int i = 0; i < types.length; ++i) {
      Node child = children.get(i).clone();
      // If the parent will expect a ScoreIterator at this position, but
      // we've got a CountIterator here, we'll perform a conversion step.
      if (ScoreIterator.class.isAssignableFrom(types[i])
              && isCountNode(children.get(i))) {
        Node feature = createSmoothingNode(child, queryParameters);
        newChildren.add(feature);
      } else {
        newChildren.add(child);
      }
    }

    return new Node(node.getOperator(), node.getNodeParameters(),
            newChildren, node.getPosition());
  }

  private Node createSmoothingNode(Node child, Parameters queryParameters) throws Exception {

    ArrayList<Node> data = new ArrayList<Node>();
    data.add(child);
    String scorerNode = queryParameters.get("scorer", globals.get("scorer", "dirichlet"));

    Node smoothed = new Node(scorerNode, data, child.getPosition());

    return smoothed;
  }

  private boolean isCountNode(Node node) throws Exception {
    NodeType nodeType = retrieval.getNodeType(node);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();

    if (FilteredIterator.class.isAssignableFrom(outputClass)) {
      return isCountNode(node.getChild(1));
    }

    return CountIterator.class.isAssignableFrom(outputClass);
  }
}
