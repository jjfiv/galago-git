// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.FilteredIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.FieldComparisonIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * For many kinds of queries, it may be preferable to not have to type
 * an explicit #feature operator around a count or extents term.  For example,
 * we want #combine(#feature:dirichlet(#counts:dog())) to be the same as
 * #combine(dog).  This transformation automatically adds the #feature:dirichlet
 * operator.
 * 
 * (12/21/2010, irmarc): Modified to annotate topdocs feature nodes, as well as generate them
 *                        if specified by construction globals.
 * (7/17/2011, irmarc): Added a check for the intersection operator. If found, makes sure to add
 *                        a -1 as the default distance, which indicates "whole doc".
 * (12/5/2011, irmarc): Added a check for wrapping extent operators with extent filters for passage
 *                        retrieval.
 *
 * @author trevor, irmarc
 */
public class ImplicitFeatureCastTraversal extends Traversal {

  Parameters globals;
  Retrieval retrieval;

  public ImplicitFeatureCastTraversal(Retrieval retrieval) {
    this.retrieval = retrieval;
    this.globals = retrieval.getGlobalParameters();
  }

  Node createSmoothingNode(Node child) throws Exception {

    /** Check if the child is an 'extents' node
     *    If so - we can replace extents with counts.
     *    This can lead to performance improvements within positions indexes
     *    as the positional data does NOT need to be read for the feature scorer to operate.
     */
    if (child.getOperator().equals("extents")) {
      child.setOperator("counts");
    }

    ArrayList<Node> data = new ArrayList<Node>();
    data.add(child);
    String scorerType = globals.get("scorer", "dirichlet");
    Node smoothed = new Node("feature", scorerType, data, child.getPosition());
    // TODO - add in smoothing globals, modifiers

    if (!globals.get("topdocs", false)) {
      return smoothed;
    }

    // If we're here, we should be adding a topdocs node
    return createTopdocsNode(smoothed);
  }

  Node createTopdocsNode(Node child) throws Exception {
    // First (and only) child should be a scoring function fieldIterator node
    if (!isScoringFunctionNode(child)) {
      return child;
    }

    // The replacement
    ArrayList<Node> children = new ArrayList<Node>();
    children.add(child);
    Node workingNode = new Node("feature", "topdocs", children, child.getPosition());

    // count node, with the information we need
    Node grandchild = child.getInternalNodes().get(0);
    NodeParameters descendantParameters = grandchild.getNodeParameters();
    NodeParameters workingParameters = workingNode.getNodeParameters();
    workingParameters.set("term", descendantParameters.getString("default"));
    workingParameters.set("loc", descendantParameters.getString("part"));
    workingParameters.set("index", globals.getString("index"));
    return workingNode;
  }

  public boolean isCountNode(Node node) throws Exception {
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

  public boolean isExtentNode(Node node) throws Exception {
    NodeType nodeType = retrieval.getNodeType(node);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();

    return ExtentIterator.class.isAssignableFrom(outputClass);
  }

  public boolean isScoringFunctionNode(Node node) throws Exception {
    NodeType nodeType = retrieval.getNodeType(node);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();

    if (FilteredIterator.class.isAssignableFrom(outputClass)) {
      return isScoringFunctionNode(node.getChild(1));
    }

    return ScoringFunctionIterator.class.isAssignableFrom(outputClass);
  }

  // Put node modification in "before", since we're not replacing the node
  @Override
  public void beforeNode(Node node) throws Exception {
    // Indicates we want "whole doc" matching
    if (node.getOperator().equals("intersect")) {
      node.getNodeParameters().set("width", -1);
      return;
    }
  }

  @Override
  public Node afterNode(Node node) throws Exception {
    // This moves the interior nodes of a field comparison operator into its
    // globals, which is the appropriate syntax.
    // Example:
    //
    // #lessThan( date #counts:6/16/1980:part=postings() ) -->
    //
    // #lessThan:0=6/16/1980( date )
    if (node.getOperator().equals("text")) {
      return node;
    }

    NodeType nt = retrieval.getNodeType(node);
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

    // Wraps an extent/count node in the default extent filter node
    // for passages
    if (isExtentNode(node)) {
      node = addExtentFilters(node);
    }

    // Determine if we need to add a scoring node
    Node scored = addScorers(node);
    return scored;
  }

  private Node addExtentFilters(Node in) throws Exception {
    if (this.globals.containsKey("passageSize") || this.globals.containsKey("passageShift")) {
      if (!this.globals.containsKey("passageSize")) {
        throw new IllegalArgumentException("passage retrieval requires an explicit passageSize parameter.");
      }

      if (!this.globals.containsKey("passageShift")) {
        throw new IllegalArgumentException("passage retrieval requires an explicit passageShift parameter.");
      }

      // replace here
      ArrayList<Node> children = new ArrayList<Node>();
      children.add(in);
      Node replacement = new Node("passagefilter", children);
      return replacement;
    } else {
     return in;
    }
  }

  public Node addScorers(Node node) throws Exception {

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
        Node feature = createSmoothingNode(child);
        newChildren.add(feature);
      } else {
        newChildren.add(child);
      }
    }

    return new Node(node.getOperator(), node.getNodeParameters(),
            newChildren, node.getPosition());
  }
}
