// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.ann.ImplementsOperator;
import org.lemurproject.galago.core.retrieval.ann.OperatorDescription;
import org.lemurproject.galago.core.retrieval.prf.ExpansionModel;
import org.lemurproject.galago.core.retrieval.prf.ExpansionModelFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

/**
 * The Relevance Model implemented as a traversal. Input query should look like:
 *
 * #prf( original query )
 *
 *
 * #rm:fbOrigWt=0.5:fbDocs=10:fbTerm=10( query )
 *
 * The outer node (the #rm operator) will be replaced with a #combine, and the
 * query submitted to the retrieval supplied at construction. The parameters
 * will then be applied for constructing the expansion.
 *
 * Supports more general RM
 *
 * @author irmarc, sjh, dietz
 */
@ImplementsOperator  (value       = "rm")
@RequiredParameters  (parameters  = {"fbDocs", "fbTerm", "fbOrigWeight", "relevanceModel"})
@OperatorDescription (description = "Relevance [Feedback] Model Operator \n" +
                                    "\t\tExpand the original query using automatically derived terms from the \n" +
                                    "\t\tspecified number of feedback terms and documents based on the relevanceModel \n" +
                                    "\t\tused.  The default RelevanceModel3 adds derived query terms to the original \n" +
                                    "\t\tquery while RelevanceModel1 replaces the original query terms with the \n" +
                                    "\t\texpansion terms only.")


public class RelevanceModelTraversal extends Traversal {

  private final Retrieval retrieval;
  private final ExpansionModel defaultExpander;

  public RelevanceModelTraversal(Retrieval retrieval) throws Exception {
    this.retrieval = retrieval;
    
    defaultExpander = ExpansionModelFactory.create(retrieval.getGlobalParameters(), retrieval);
  }

  @Override
  public Node afterNode(Node originalNode, Parameters queryParams) throws Exception {
    
    if (originalNode.getOperator().equals("rm") == false) {
      return originalNode;
    }
    
    ExpansionModel em = defaultExpander;
    if(queryParams.containsKey("relevanceModel")){
      em = ExpansionModelFactory.create(queryParams, retrieval);
    }
    
    // strip the #rm operator
    Node newRoot = new Node("combine", originalNode.getNodeParameters(), originalNode.getInternalNodes(), originalNode.getPosition());
    Node expanded = em.expand(newRoot, queryParams);
    
    return expanded;
  }

  @Override
  public void beforeNode(Node object, Parameters queryParams) throws Exception {
  }
}
