/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class ProximityDFRTraversal extends Traversal {

  private int windowSizeDefault;
  private double twDefault;
  private double cDefault;
//  private double pwDefault;
  private double cpDefault;
  private boolean sequentialDefault;
  private String termScoringModelDefault;
  private String proxScoringModelDefault;

  public ProximityDFRTraversal(Retrieval ret) {
    this(ret.getGlobalParameters());
  }
  
  public ProximityDFRTraversal(Parameters p) {
    this.sequentialDefault = p.get("pdfrSeq", true);
    this.twDefault = p.get("termLambda", 1.0);
    this.cDefault = p.get("c", 6.0);

    // this.pwDefault = p.get("proximityLambda", 1.0 - twDefault);
    this.cpDefault = p.get("cp", 0.05);
    
    this.termScoringModelDefault = p.get("pdfrTerm", "pl2");
    this.proxScoringModelDefault = p.get("pdfrProx", "bil2");
    this.windowSizeDefault = (int) p.get("windowSize", 5);
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
      double pw = 1.0 - tw ;
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
        Node scorer = new Node(termScoringModel);
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
      Node scorer = new Node(proxScoringModel);
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
        Node scorer = new Node(proxScoringModel);
        scorer.getNodeParameters().set("c", cp);
        scorer.addChild(StructuredQuery.parse("#lengths:document:part=lengths()"));
        Node uw = new Node("unordered");
        uw.getNodeParameters().set("default", windowSize);
        uw.addChild(internalNodes.get(i));
        uw.addChild(internalNodes.get(j));
        scorer.addChild(uw);
        proxy.addChild(scorer);
      }
    }

    return proxy;
  }
}
