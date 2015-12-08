// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ann.ImplementsOperator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.utility.Parameters;

import java.util.ArrayList;
import java.util.List;

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
@ImplementsOperator(operator = "sdm")
public class SequentialDependenceTraversal extends Traversal {

  private final int windowLimitDefault;
  private final double unigramDefault;
  private final double orderedDefault;
  private final double unorderedDefault;
  private final Retrieval r;

  private final String odOp;
  private final int odWidth;
  private final String uwOp;
  private final int uwWidth;

  private final boolean fasterOperators;
  
  public SequentialDependenceTraversal(Retrieval retrieval) {
    r = retrieval;
    Parameters parameters = retrieval.getGlobalParameters();
    unigramDefault = parameters.get("uniw", 0.8);
    orderedDefault = parameters.get("odw", 0.15);
    unorderedDefault = parameters.get("uww", 0.05);
    windowLimitDefault = (int) parameters.get("windowLimit", 2);

    // this parameter controls the use of specialized bigram and ubigram operators
    fasterOperators = parameters.get("fast", false);

    odOp = parameters.get("sdm.od.op", "ordered");
    odWidth = (int) parameters.get("sdm.od.width", 1);
    
    uwOp = parameters.get("sdm.uw.op", "unordered");
    uwWidth = (int) parameters.get("sdm.uw.width", 4);
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

      // formatting is ok - now reassemble
      // unigrams go as-is
      Node unigramNode = new Node("combine", Node.cloneNodeList(children));
      if (children.size() == 1) {
        return unigramNode;
      }

      boolean fast = np.get("fast", qp.get("fast", fasterOperators));

      // ordered and unordered can go at the same time
      ArrayList<Node> ordered = new ArrayList<>();
      ArrayList<Node> unordered = new ArrayList<>();

      for (int n = 2; n <= windowLimit; n++) {
        for (int i = 0; i < (children.size() - n + 1); i++) {
          List<Node> seq = children.subList(i, i + n);

          String orderedOp = this.odOp;
          String unorderedOp = this.uwOp;

          if(fast && n == 2) {
            orderedOp = "bigram";
            unorderedOp = "ubigram";
          }

          ordered.add(new Node(qp.get("sdm.od.op", orderedOp), new NodeParameters(qp.get("sdm.od.width", odWidth)), Node.cloneNodeList(seq)));
          unordered.add(new Node(qp.get("sdm.uw.op", unorderedOp), new NodeParameters(qp.get("sdm.uw.width", uwWidth) * seq.size()), Node.cloneNodeList(seq)));
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
      return new Node("combine", weights, immediateChildren, original.getPosition());
    } else {
      return original;
    }
  }
}
