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
 * Transforms a #sdm operator into a full expansion of the sequential dependence
 * model. That means:
 *
 * #seqdep( #text:term1() #text:term2() ... termk ) -->
 *
 * #weight ( 0.8 #combine ( term1 term2 ... termk) 0.15 #combine ( #od(term1
 * term2) #od(term2 term3) ... #od(termk-1 termk) ) 0.05 #combine ( #uw8(term
 * term2) ... #uw8(termk-1 termk) ) )
 *
 *
 *
 * @author irmarc
 */
public class SequentialDependenceTraversal extends Traversal {

  private int windowLimitDefault;
  private double unigramDefault;
  private double orderedDefault;
  private double unorderedDefault;
  private Retrieval r;

  public SequentialDependenceTraversal(Retrieval retrieval) {
    r = retrieval;
    Parameters parameters = retrieval.getGlobalParameters();
    unigramDefault = parameters.get("uniw", 0.8);
    orderedDefault = parameters.get("odw", 0.15);
    unorderedDefault = parameters.get("uww", 0.05);
    windowLimitDefault = (int) parameters.get("windowLimit", 2);
  }

  @Override
  public void beforeNode(Node original, Parameters qp) throws Exception {
  }

  @Override
  public Node afterNode(Node original, Parameters qp) throws Exception {
    if (original.getOperator().equals("sdm")
            || original.getOperator().equals("seqdep")) {
      // get to work

      double unigramW = qp.get("uniw", unigramDefault);
      double orderedW = qp.get("odw", orderedDefault);
      double unorderedW = qp.get("uww", unorderedDefault);
      int windowLimit = (int) qp.get("windowLimit", windowLimitDefault);

      NodeParameters np = original.getNodeParameters();
      unigramW = np.get("uniw", unigramW);
      orderedW = np.get("odw", orderedW);
      unorderedW = np.get("uww", unorderedW);
      windowLimit = (int) np.get("windowLimit", windowLimit);

      List<Node> children = original.getInternalNodes();

//      //  TODO: Remove this gross hack and use a proper dependency-graph builder.
//      qp.set("seqdep", true);
//      qp.set("numberOfTerms", children.size());

      // formatting is ok - now reassemble
      // unigrams go as-is
      Node unigramNode = new Node("combine", Node.cloneNodeList(children));
      if (children.size() == 1) {
        return unigramNode;
      }

      // ordered and unordered can go at the same time
      ArrayList<Node> ordered = new ArrayList<Node>();
      ArrayList<Node> unordered = new ArrayList<Node>();


      for (int n = 2; n <= windowLimit; n++) {
        for (int i = 0; i < (children.size() - n + 1); i++) {
          List<Node> seq = children.subList(i, i + n);
          ordered.add(new Node("ordered", new NodeParameters(1), Node.cloneNodeList(seq)));
          unordered.add(new Node("unordered", new NodeParameters(4 * seq.size()), Node.cloneNodeList(seq)));
        }
      }

      Node orderedWindowNode = new Node("combine", ordered);
      Node unorderedWindowNode = new Node("combine", unordered);

      NodeParameters weights = new NodeParameters();
      ArrayList<Node> immediateChildren = new ArrayList<Node>();

      // unigrams - 0.80
      weights.set("0", unigramW);
      immediateChildren.add(unigramNode);

      // ordered
      weights.set("1", orderedW);
      immediateChildren.add(orderedWindowNode);

      // unordered
      weights.set("2", unorderedW);
      immediateChildren.add(unorderedWindowNode);

      // Finally put them all inside a combine node w/ the weights
      Node outerweight = new Node("combine", weights, immediateChildren, original.getPosition());
      return outerweight;
    } else {
      return original;
    }
  }
}
