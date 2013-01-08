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
  private final Parameters queryParams;
  private int windowSize;
  private double twDefault;
  private double cDefault;
  private double pwDefault;
  private double cpDefault;
  private boolean sequential;
  private String termScoringModel;
  private String proxScoringModel;

  public ProximityDFRTraversal(Retrieval ret, Parameters queryParams) {
    this.ret = ret;
    this.queryParams = queryParams;

    this.sequential = ret.getGlobalParameters().get("pdfrSeq", true);
    this.twDefault = ret.getGlobalParameters().get("termLambda", 1.0);
    this.cDefault = ret.getGlobalParameters().get("c", 6.0);
    this.pwDefault = ret.getGlobalParameters().get("proximityLambda", 1.0);
    this.cpDefault = ret.getGlobalParameters().get("cp", 0.05);
    this.termScoringModel = ret.getGlobalParameters().get("pdfrTerm", "bil2");
    this.proxScoringModel = ret.getGlobalParameters().get("pdfrProx", "bil2");
    this.windowSize = (int) ret.getGlobalParameters().get("windowSize", 5);

    this.sequential = queryParams.get("pdfrSeq", this.sequential);
    this.twDefault = queryParams.get("termLambda", twDefault);
    this.cDefault = queryParams.get("c", cDefault);
    this.pwDefault = queryParams.get("termLambda", pwDefault);
    this.cpDefault = queryParams.get("cp", cpDefault);
    this.termScoringModel = queryParams.get("pdfrTerm", termScoringModel);
    this.proxScoringModel = queryParams.get("pdfrProx", proxScoringModel);
    this.windowSize = (int) queryParams.get("windowSize", windowSize);
  }

  @Override
  public Node afterNode(Node original) throws Exception {
    if (original.getOperator().equals("pdfr")) {
      Node root = new Node("combine");
      root.getNodeParameters().set("norm", false);

      // set term and proximity weights
      root.getNodeParameters().set("0", twDefault);
      root.getNodeParameters().set("1", pwDefault);

      // build unigram node:
      Node unigramNode = new Node("combine");
      for (int i = 0; i < original.numChildren(); i++) {
        Node scorer = new Node("feature", termScoringModel);
        scorer.getNodeParameters().set("c", this.cDefault);
        scorer.addChild(StructuredQuery.parse("#lengths:document:part=lengths()"));
        scorer.addChild(original.getChild(i));
        unigramNode.addChild(scorer);
      }
      root.addChild(unigramNode);

      // only add 
      if (original.numChildren() > 1) {
        Node proxNode;
        if (sequential) {
          proxNode = createSequentialProxNode(original.getInternalNodes());
        } else {
          proxNode = createFullProxNode(original.getInternalNodes());
        }
        root.addChild(proxNode);
      }

      return root;
    }
    return original;
  }

  @Override
  public void beforeNode(Node object) throws Exception {
    // do nothing
  }

  private Node createSequentialProxNode(List<Node> internalNodes) {
    Node proxy = new Node("combine");

    for (int i = 0; i < internalNodes.size() - 1; i++) {
      Node scorer = new Node("feature", this.proxScoringModel);
      scorer.getNodeParameters().set("c", this.cpDefault);
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

  private Node createFullProxNode(List<Node> internalNodes) {
    Node proxy = new Node("combine");

    for (int i = 0; i < internalNodes.size(); i++) {
      for (int j = i + 1; j < internalNodes.size(); j++) {
        Node scorer = new Node("feature", this.proxScoringModel);
        scorer.getNodeParameters().set("c", this.cpDefault);
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
