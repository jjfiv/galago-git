/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class ProximityDFRTraversal extends Traversal {

  private final Retrieval ret;
  private int windowSizeDefault;
  private double twDefault;
  private double cDefault;
  private double pwDefault;
  private double cpDefault;
  private boolean sequentialDefault;
  private String termScoringModelDefault;
  private String proxScoringModelDefault;

  public ProximityDFRTraversal(Retrieval ret) {
    this.ret = ret;

    this.sequentialDefault = ret.getGlobalParameters().get("pdfrSeq", true);
    this.twDefault = ret.getGlobalParameters().get("termLambda", 1.0);
    this.cDefault = ret.getGlobalParameters().get("c", 6.0);
    this.pwDefault = ret.getGlobalParameters().get("proximityLambda", 1.0);
    this.cpDefault = ret.getGlobalParameters().get("cp", 0.05);
    this.termScoringModelDefault = ret.getGlobalParameters().get("pdfrTerm", "pl2");
    this.proxScoringModelDefault = ret.getGlobalParameters().get("pdfrProx", "bil2");
    this.windowSizeDefault = (int) ret.getGlobalParameters().get("windowSize", 5);
  }

  @Override
  public void beforeNode(Node object, Parameters qp) throws Exception {
    // do nothing
  }

  @Override
  public Node afterNode(Node original, Parameters queryParams) throws Exception {
    if (original.getOperator().equals("pdfr")) {

      boolean sequential = queryParams.get("pdfrSeq", this.sequentialDefault);
      double tw = queryParams.get("termLambda", twDefault);
      double c = queryParams.get("c", cDefault);
      double pw = queryParams.get("termLambda", pwDefault);
      double cp = queryParams.get("cp", cpDefault);
      String termScoringModel = queryParams.get("pdfrTerm", termScoringModelDefault);
      String proxScoringModel = queryParams.get("pdfrProx", proxScoringModelDefault);
      int windowSize = (int) queryParams.get("windowSize", windowSizeDefault);

      Node root = new Node("combine");
      root.getNodeParameters().set("norm", false);

      // set term and proximity weights
      root.getNodeParameters().set("0", tw);
      root.getNodeParameters().set("1", pw);

      // build unigram node:
      Node unigramNode = new Node("combine");
      for (int i = 0; i < original.numChildren(); i++) {
        Node scorer = new Node("feature", termScoringModel);
        scorer.getNodeParameters().set("c", c);
        scorer.addChild(StructuredQuery.parse("#lengths:document:part=lengths()"));
        scorer.addChild(original.getChild(i));
        unigramNode.addChild(scorer);
      }
      root.addChild(unigramNode);

      // only add 
      if (original.numChildren() > 1) {
        Node proxNode;
        if (sequential) {
          proxNode = createSequentialProxNode(original.getInternalNodes(), proxScoringModel, cp, windowSize);
        } else {
          proxNode = createFullProxNode(original.getInternalNodes(), proxScoringModel, cp, windowSize);
        }
        root.addChild(proxNode);
      }

      return root;
    }
    return original;
  }

  private Node createSequentialProxNode(List<Node> internalNodes, String proxScoringModel, double cp, int windowSize) {
    Node proxy = new Node("combine");

    for (int i = 0; i < internalNodes.size() - 1; i++) {
      Node scorer = new Node("feature", proxScoringModel);
      scorer.getNodeParameters().set("c", cp);
      scorer.addChild(StructuredQuery.parse("#lengths:document:part=lengths()"));
      Node od = new Node("ordered");
      od.getNodeParameters().set("default", windowSize);
      od.addChild(internalNodes.get(i));
      od.addChild(internalNodes.get(i + 1));
      scorer.addChild(od);
      proxy.addChild(scorer);
    }

    return proxy;
  }

  private Node createFullProxNode(List<Node> internalNodes, String proxScoringModel, double cp, int windowSize) {
    Node proxy = new Node("combine");

    for (int i = 0; i < internalNodes.size(); i++) {
      for (int j = i + 1; j < internalNodes.size(); j++) {
        Node scorer = new Node("feature", proxScoringModel);
        scorer.getNodeParameters().set("c", cp);
        scorer.addChild(StructuredQuery.parse("#lengths:document:part=lengths()"));
        Node od = new Node("unordered");
        od.getNodeParameters().set("default", windowSize);
        od.addChild(internalNodes.get(i));
        od.addChild(internalNodes.get(j));
        scorer.addChild(od);
        proxy.addChild(scorer);
      }
    }

    return proxy;
  }
}
