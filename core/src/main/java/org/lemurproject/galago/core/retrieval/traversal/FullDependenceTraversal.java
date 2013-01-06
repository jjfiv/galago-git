// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.MalformedQueryException;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Transforms a #fulldep operator into a full expansion of the sequential
 * dependence model. That means:
 *
 * #fulldep( #text:term1() #text:term2() ... termk ) -->
 *
 * #weight ( 0.8 #combine ( term1 term2 ... termk) 0.15 #combine ( #od(term1
 * term2) #od(term2 term3) #od(term1 term3) ... #od(term1 ... termk) ) 0.05
 * #combine ( #uw8(term1 term2) #uw8(term1 term2) #uw8(term1 term3) ...
 * #uw8(term1 ... termk) ) )
 *
 *
 *
 * @author sjh
 */
public class FullDependenceTraversal extends Traversal {

  private int defaultWindowLimit;
  private double unigramDefault;
  private double orderedDefault;
  private double unorderedDefault;

  public FullDependenceTraversal(Retrieval retrieval, Parameters queryParameters) {
    Parameters parameters = retrieval.getGlobalParameters();
    unigramDefault = parameters.get("uniw", 0.8);
    orderedDefault = parameters.get("odw", 0.15);
    unorderedDefault = parameters.get("uww", 0.05);
    defaultWindowLimit = (int) parameters.get("windowLimit", 3);

    unigramDefault = queryParameters.get("uniw", unigramDefault);
    orderedDefault = queryParameters.get("odw", orderedDefault);
    unorderedDefault = queryParameters.get("uww", unorderedDefault);
    defaultWindowLimit = (int) queryParameters.get("windowLimit", defaultWindowLimit);
  }

  @Override
  public void beforeNode(Node original) throws Exception {
  }

  @Override
  public Node afterNode(Node original) throws Exception {
    if (original.getOperator().equals("fdm")
            || original.getOperator().equals("fulldep")) {

      int windowLimit = (int) original.getNodeParameters().get("windowLimit", defaultWindowLimit);

      // First check format - should only contain text node children
      List<Node> children = original.getInternalNodes();
      for (Node child : children) {
        if (child.getOperator().equals("text") == false) {
          throw new MalformedQueryException("fulldep operator needs text-only children");
        }
      }

      // formatting is ok - now reassemble
      // unigrams go as-is
      Node unigramNode = new Node("combine", Node.cloneNodeList(children));

      // if we only have one child, return the unigram node.
      if (children.size() == 1) {
        return unigramNode;
      }

      // ordered and unordered can go at the same time
      ArrayList<Node> ordered = new ArrayList<Node>();
      ArrayList<Node> unordered = new ArrayList<Node>();

      ArrayList<ArrayList<Node>> nodePowerSet = powerSet(new ArrayList(children), windowLimit);

      for (ArrayList<Node> set : nodePowerSet) {
        if ((windowLimit < 2) || (windowLimit >= set.size())) {
          if (set.size() >= 2) {
            int uwSize = 4 * set.size();
            ordered.add(new Node("ordered", new NodeParameters(1), Node.cloneNodeList(set)));
            unordered.add(new Node("unordered", new NodeParameters(uwSize), Node.cloneNodeList(set)));
          }
        }
      }

      Node orderedWindowNode = new Node("combine", ordered);
      Node unorderedWindowNode = new Node("combine", unordered);

      // now get the weights for each component, and add to immediate children
      NodeParameters parameters = original.getNodeParameters();
      double uni = parameters.get("uniw", unigramDefault);
      double odw = parameters.get("odw", orderedDefault);
      double uww = parameters.get("uww", unorderedDefault);

      NodeParameters weights = new NodeParameters();
      ArrayList<Node> immediateChildren = new ArrayList<Node>();

      // unigrams - 0.80
      weights.set("0", uni);
      immediateChildren.add(unigramNode);

      // ordered
      weights.set("1", odw);
      immediateChildren.add(orderedWindowNode);

      // unordered
      weights.set("2", uww);
      immediateChildren.add(unorderedWindowNode);

      // Finally put them all inside a comine node w/ the weights
      Node outerweight = new Node("combine", weights, immediateChildren, original.getPosition());
      return outerweight;
    } else {
      return original;
    }
  }

  private ArrayList<ArrayList<Node>> powerSet(ArrayList<Node> children, int windowLimit) {
    // base case
    ArrayList<ArrayList<Node>> ps = new ArrayList();

    if (children.isEmpty()) {
      ps.add(new ArrayList());
    } else {
      Node n = children.remove(0);
      ArrayList<ArrayList<Node>> subps = powerSet(children, windowLimit);
      for (ArrayList<Node> set : subps) {
        if (windowLimit < 2 || set.size() < windowLimit) {
          // add a clone of the original
          ps.add(new ArrayList(set));
          // add the original + node n
          set.add(0, n);
          ps.add(set);
        } else {
          // otherwise just add the original set -- pass through
          ps.add(set);
        }
      }
    }
    return ps;
  }
}
