// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Set;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.scoring.ExpansionModel;
import org.lemurproject.galago.core.scoring.RelevanceModel;
import java.util.Arrays;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.WordLists;
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

  private Parameters globalParameters;
  private Parameters queryParams;
  private Retrieval retrieval;
  private double defFbOrigWt;
  private double defFbDocs;
  private double defFbTerms;
  private boolean defFb2Pass;
  private Parameters fbParams;
  private Parameters fb2PassParams;

  public RelevanceModelTraversal(Retrieval retrieval, Parameters queryParams) throws IOException {
    this.queryParams = queryParams;
    this.retrieval = retrieval;

    this.globalParameters = retrieval.getGlobalParameters();

    defFbOrigWt = queryParams.get("fbOrigWt", globalParameters.get("fbOrigWt", 0.85));
    defFbDocs = queryParams.get("fbDocs", globalParameters.get("fbDocs", 10.0));
    defFbTerms = queryParams.get("fbTerms", globalParameters.get("fbTerms", 5.0));

    // allows two passes to collect feedback documents
    defFb2Pass = queryParams.get("fb2Pass", globalParameters.get("fb2Pass", false));

    fbParams = new Parameters();
    if (globalParameters.containsKey("fbParams")) {
      fbParams.copyFrom(globalParameters.getMap("fbParams"));
    }
    if (queryParams.containsKey("fbParams")) {
      fbParams.copyFrom(queryParams.getMap("fbParams"));
    }

    fb2PassParams = new Parameters();
    if (globalParameters.containsKey("fbParams2Pass")) {
      fb2PassParams.copyFrom(globalParameters.getMap("fbParams2Pass"));
    }
    if (queryParams.containsKey("fbParams2Pass")) {
      fb2PassParams.copyFrom(queryParams.getMap("fbParams2Pass"));
    }
  }

  @Override
  public Node afterNode(Node originalNode) throws Exception {
    if (originalNode.getOperator().equals("rm") == false) {
      return originalNode;
    }

    // Kick off the inner query
    NodeParameters parameters = originalNode.getNodeParameters();
    double fbOrigWt = parameters.get("fbOrigWt", defFbOrigWt);
    int fbDocs;
    // doubles allow learning module to operate over these parameters. -- default behaviour is to round to nearest integer.
    if (parameters.isLong("fbDocs")) {
      fbDocs = (int) parameters.getLong("fbDocs");
    } else {
      fbDocs = (int) Math.round(parameters.get("fbDocs", defFbDocs));
    }
    int fbTerms;
    if (parameters.isLong("fbTerms")) {
      fbTerms = (int) parameters.getLong("fbTerms");
    } else {
      fbTerms = (int) Math.round(parameters.get("fbTerms", defFbTerms));
    }

    // check parameters
    if (fbDocs <= 0) {
      return originalNode;
    }

    if (fbTerms <= 0) {
      return originalNode;
    }

    String operator = "combine";
    Node combineNode = new Node(operator, new NodeParameters(), Node.cloneNodeList(originalNode.getInternalNodes()), originalNode.getPosition());
    Node combineTmpNode = new Node(operator, new NodeParameters(), Node.cloneNodeList(originalNode.getInternalNodes()), originalNode.getPosition());
    List<ScoredDocument> initialResults = new ArrayList<ScoredDocument>();

    Parameters localRmParameters = new Parameters();
//    localRmParameters.copyFrom(queryParams);
    localRmParameters.set("requested", fbDocs);
    localRmParameters.copyFrom(fbParams); // can override requested to ensure larger pool for second pass.
    
    Node transformedCombineNode = retrieval.transformQuery(combineNode, localRmParameters);
    initialResults.addAll(Arrays.asList(retrieval.runQuery(transformedCombineNode, localRmParameters)));

    // if we are doing two passes (e.g. for passage retrieval
    if (defFb2Pass) {
      Parameters localRm2Parameters = new Parameters();
//      localRm2Parameters.copyFrom(queryParams);
      localRm2Parameters.copyFrom(fb2PassParams);
      // can't override requested
      localRm2Parameters.set("requested", fbDocs);
      
      ArrayList<String> workingSet = new ArrayList<String>();
      for (ScoredDocument doc : initialResults) {
        workingSet.add(doc.documentName);
      }

      localRm2Parameters.set("working", workingSet);

      transformedCombineNode = retrieval.transformQuery(combineTmpNode, localRm2Parameters);

      // we've now got a new set of results -- overwrite old ones.
      initialResults = Arrays.asList(retrieval.runQuery(transformedCombineNode, localRm2Parameters));
      
      // pass these parameters to the RM scorer -- just in case.
      localRmParameters = localRm2Parameters;
    }

    ExpansionModel rModel = createRelevanceModel(localRmParameters, retrieval);
    rModel.initialize();

    Set<String> stopwords = WordLists.getWordList("rmstop");
    Set<String> queryTerms = StructuredQuery.findQueryTerms(combineNode);

    Node newRoot = null;
    Node expansionNode;

    expansionNode = rModel.generateExpansionQuery(initialResults, fbTerms, queryTerms, stopwords);
    
    // if we got nothing back -- return a combine node.
    if(expansionNode == null){
      return combineNode;
    }
    
    NodeParameters expParams = new NodeParameters();
    expParams.set("0", fbOrigWt);
    expParams.set("1", 1.0 - fbOrigWt);
    ArrayList<Node> newChildren = new ArrayList<Node>();
    newChildren.add(combineNode);
    newChildren.add(expansionNode);
    newRoot = new Node("combine", expParams, newChildren, originalNode.getPosition());

    rModel.cleanup();
    return newRoot;
  }

  protected ExpansionModel createRelevanceModel(Parameters parameters, Retrieval r) {
    try {
      if (globalParameters.containsKey("relevanceModel")) {
        String modelName = globalParameters.getString("relevanceModel");
        System.out.println("Instantiating Relevance model " + modelName);
        Class clazz = Class.forName(modelName);
        Constructor cons = clazz.getConstructor(Parameters.class, Retrieval.class);
        ExpansionModel relevanceModel = (ExpansionModel) cons.newInstance(parameters, r);
        return relevanceModel;
      }
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }

//    return new GeneralRelevanceModel(parameters, r);
    // now obsolete rm model
    return new RelevanceModel(parameters, retrieval);
  }

  @Override
  public void beforeNode(Node object) throws Exception {
  }
}
