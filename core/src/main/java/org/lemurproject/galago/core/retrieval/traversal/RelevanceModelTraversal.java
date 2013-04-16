// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.scoring.RelevanceModel;
import java.util.Arrays;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.WordLists;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * The Relevance Model implemented as a traversal. Query should look like:
 *
 * #rm:fbOrigWt=0.5:fbDocs=10:fbTerms=10( query )
 *
 * The outer node (the #rm operator) will be replaced with a #combine, and the
 * query submitted to the retrieval supplied at construction. The parameters
 * will then be applied for constructing the expansion.
 *
 * @author irmarc, sjh
 */
public class RelevanceModelTraversal extends Traversal {

  Parameters globalParameters;
  Parameters queryParams;
  Parameters availableParts;
  Retrieval retrieval;
  double defFbOrigWt;
  double defFbDocs;
  double defFbTerms;

  public RelevanceModelTraversal(Retrieval retrieval, Parameters queryParams) throws IOException {
    this.queryParams = queryParams;
    this.retrieval = retrieval;

    this.globalParameters = retrieval.getGlobalParameters();
    this.availableParts = retrieval.getAvailableParts();

    defFbOrigWt = queryParams.get("fbOrigWt", globalParameters.get("fbOrigWt", 0.5));
    defFbDocs = queryParams.get("fbDocs", globalParameters.get("fbDocs", 10.0));
    defFbTerms = queryParams.get("fbTerms", globalParameters.get("fbTerms", 10.0));
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
    ArrayList<ScoredDocument> initialResults = new ArrayList<ScoredDocument>();

    // Only get as many as we need
    Parameters localParameters = queryParams.clone();
    localParameters.set("requested", fbDocs);

    Node transformedCombineNode = retrieval.transformQuery(combineNode, localParameters);
    initialResults.addAll(Arrays.asList(retrieval.runQuery(transformedCombineNode, localParameters)));
    localParameters.set("parts", this.availableParts);

    RelevanceModel rModel = new RelevanceModel(localParameters, retrieval);
    rModel.initialize();

    Set<String> stopwords = WordLists.getWordList("rmstop");
    Set<String> queryTerms = StructuredQuery.findQueryTerms(combineNode);

    Node newRoot = null;
    Node expansionNode;

    expansionNode = rModel.generateExpansionQuery(initialResults, fbTerms, queryTerms, stopwords);
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

  @Override
  public void beforeNode(Node object) throws Exception {
  }
}
