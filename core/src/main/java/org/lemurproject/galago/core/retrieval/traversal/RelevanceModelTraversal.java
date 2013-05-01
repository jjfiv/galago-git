// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.prf.ExpansionModel;
import org.lemurproject.galago.core.retrieval.prf.ExpansionModelFactory;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * The Relevance Model implemented as a traversal. Input query should look like:
 *
 * #prf( original query )
 *
 *
 * #rm:fbOrigWt=0.5:fbDocs=10:fbTerms=10( query )
 *
 * The outer node (the #rm operator) will be replaced with a #combine, and the
 * query submitted to the retrieval supplied at construction. The parameters
 * will then be applied for constructing the expansion.
 *
 * Supports more general RM
 *
 * @author irmarc, sjh, dietz
 */
public class RelevanceModelTraversal extends Traversal {

  private final Retrieval retrieval;
  private final ExpansionModel defaultExpander;

  public RelevanceModelTraversal(Retrieval retrieval) throws Exception {
    this.retrieval = retrieval;
    
    defaultExpander = ExpansionModelFactory.instance(retrieval.getGlobalParameters(), retrieval);
  }

  @Override
  public Node afterNode(Node originalNode, Parameters queryParams) throws Exception {
    
    if (originalNode.getOperator().equals("rm") == false) {
      return originalNode;
    }
    
    ExpansionModel em = defaultExpander;
    if(queryParams.containsKey("relevanceModel")){
      em = ExpansionModelFactory.instance(queryParams, retrieval);
    }
    
    Node expanded = em.expand(originalNode, queryParams);
    
    return expanded;
  }

  @Override
  public void beforeNode(Node object, Parameters queryParams) throws Exception {
  }
}
