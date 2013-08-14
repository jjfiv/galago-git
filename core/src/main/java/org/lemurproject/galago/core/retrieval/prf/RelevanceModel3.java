/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.prf;

import java.util.logging.Logger;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh, dmf
 */
public class RelevanceModel3 implements ExpansionModel {

  private static final Logger logger = Logger.getLogger("RM3");
  // sufficient
  private RelevanceModel1 rm1;
  private double defaultFbOrigWeight;


  public RelevanceModel3(Retrieval r) throws Exception {
      
    this.rm1 = new RelevanceModel1(r);
    defaultFbOrigWeight = r.getGlobalParameters().get("fbOrigWeight", 0.85);
  }

  @Override
  public Node expand(Node root, Parameters queryParameters) throws Exception {

    double fbOrigWeight = queryParameters.get("fbOrigWeight", defaultFbOrigWeight);
    if (fbOrigWeight == 1.0) {
      logger.info("fbOrigWeight is invalid (1.0)");
      return root;
    }
    Node expNode = rm1.expand(root, queryParameters);
    if (expNode == root) {
        // no expansion performed
        return root;
    }
    Node rm3 = new Node("combine");
    rm3.addChild(root);
    rm3.addChild(expNode);
    rm3.getNodeParameters().set("0", fbOrigWeight);
    rm3.getNodeParameters().set("1", 1.0 - fbOrigWeight);
    return rm3;
  }
}
