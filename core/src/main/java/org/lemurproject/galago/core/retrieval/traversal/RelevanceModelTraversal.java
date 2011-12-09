// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.util.CallTable;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.core.scoring.RelevanceModel;
import org.lemurproject.galago.core.scoring.RelevanceModel.Gram;
import org.lemurproject.galago.core.scoring.WeightedTerm;
import org.lemurproject.galago.tupleflow.Utility;
import gnu.trove.TDoubleArrayList;
import java.util.Arrays;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.tartarus.snowball.ext.englishStemmer;

/**
 * The Relevance Model implemented as a traversal. Query should look like:
 *
 *  #rm:fbOrigWt=0.5:fbDocs=10:fbTerms=10( query )
 *
 * The outer node (the #rm operator) will be replaced with a #combine, and the 
 * query submitted to the retrieval supplied at construction. The parameters
 * will then be applied for constructing the expansion.
 *
 * @author irmarc
 */
public class RelevanceModelTraversal implements Traversal {

  englishStemmer stemmer = null;
  Retrieval retrieval;
  Parameters globalParameters;
  Parameters availableParts;

  public RelevanceModelTraversal(Retrieval retrieval) throws IOException {
    this.retrieval = retrieval;
    this.globalParameters = retrieval.getGlobalParameters();
    this.availableParts = retrieval.getAvailableParts();
    if (globalParameters.get("stemming", true)) {
      stemmer = new englishStemmer();
    }
  }

  public Node afterNode(Node originalNode) throws Exception {
    if (originalNode.getOperator().equals("rm") == false) {
      return originalNode;
    }

    // Kick off the inner query
    NodeParameters parameters = originalNode.getNodeParameters();
    int usingMaxScore = (int) parameters.get("maxscore", 0);
    int fbDocs = (int) parameters.get("fbDocs", 10);
    String operator = (usingMaxScore > 0) ? "maxscore" : "combine";
    Node combineNode = new Node(operator, new NodeParameters(), Node.cloneNodeList(originalNode.getInternalNodes()), originalNode.getPosition());
    ArrayList<ScoredDocument> initialResults = new ArrayList<ScoredDocument>();

    // Only get as many as we need
    Parameters localParameters = globalParameters.clone();
    localParameters.set("requested", fbDocs);

    // This pass doesn't count
    CallTable.turnOff();
    Node transformedCombineNode = retrieval.transformQuery(combineNode);
    initialResults.addAll(Arrays.asList(retrieval.runQuery(transformedCombineNode, localParameters)));
    localParameters.set("parts", this.availableParts);
    RelevanceModel rModel = new RelevanceModel(localParameters, retrieval);
    rModel.initialize();
    double fbOrigWt = parameters.get("fbOrigWt", 0.5);
    int fbTerms = (int) parameters.get("fbTerms", 10);
    HashSet<String> stopwords = Utility.readStreamToStringSet(getClass().getResourceAsStream("/stopwords/inquery"));
    Set<String> queryTerms = StructuredQuery.findQueryTerms(combineNode);
    stopwords.addAll(queryTerms);

    CallTable.turnOn();

    Node newRoot = null;
    Node expansionNode;

    if (usingMaxScore == 0) {
      expansionNode = rModel.generateExpansionQuery(initialResults, fbTerms, stopwords);
      NodeParameters expParams = new NodeParameters();
      expParams.set("0", fbOrigWt);
      expParams.set("1", 1 - fbOrigWt);
      ArrayList<Node> newChildren = new ArrayList<Node>();
      newChildren.add(combineNode);
      newChildren.add(expansionNode);
      newRoot = new Node("combine", expParams, newChildren, originalNode.getPosition());

    } else if (usingMaxScore == 1) {
      // push scores down in order to wrap everything in maxscore -- definitely better this way
      NodeParameters expParams = new NodeParameters();
      // set organization if need be.
      if (parameters.containsKey("sort")) {
        expParams.set("sort", parameters.getString("sort"));
      }

      ArrayList<Node> newChildren = new ArrayList<Node>();

      // original terms first
      List<Node> oldChildren = originalNode.getInternalNodes();
      parameters = originalNode.getNodeParameters();
      double weightSum = 0;
      double[] weights = new double[oldChildren.size()];
      for (int i = 0; i < oldChildren.size(); i++) {
        double weight = parameters.get(Integer.toString(i), 1.0D);
        weightSum += weight;
        weights[i] = weight;
        newChildren.add(oldChildren.get(i));
      }
      // now the weights
      for (int i = 0; i < weights.length; i++) {
        expParams.set(Integer.toString(i), (fbOrigWt * weights[i] / weightSum));
      }

      int position = weights.length;

      List<WeightedTerm> scored = rModel.generateGrams(initialResults.subList(0, fbDocs));
      // Do the same for the expansion children
      weightSum = 0;
      int expanded = 0;
      TDoubleArrayList weightList = new TDoubleArrayList();
      for (int i = 0; i < scored.size() && expanded < fbTerms; i++) {
        Gram g = (RelevanceModel.Gram) scored.get(i);
        if (stopwords.contains(g.term)) {
          continue;
        }
        Node child = TextPartAssigner.assignPart(new Node("text", g.term), availableParts);
        child.getNodeParameters().set("mod", "topdocs");
        weightSum += g.score;
        weightList.add(g.score);
        newChildren.add(child);
        expanded++;
      }
      // now the weights
      weights = weightList.toNativeArray();
      double factor = (1.0 - fbOrigWt) / weightSum;
      for (int i = 0; i < weights.length; i++) {
        expParams.set(Integer.toString(i + position), factor * weights[i]);
      }

      // And wrap it up
      newRoot = new Node("maxscore", expParams, newChildren, originalNode.getPosition());
      newRoot = retrieval.transformQuery(newRoot);
    } else if (usingMaxScore == 2) {
      expansionNode = rModel.generateExpansionQuery(initialResults, fbTerms, stopwords);
      NodeParameters expParams = new NodeParameters();
      expParams.set("0", fbOrigWt);
      expParams.set("1", 1 - fbOrigWt);
      ArrayList<Node> newChildren = new ArrayList<Node>();
      Node replacement = new Node("combine", combineNode.getNodeParameters(), combineNode.getInternalNodes(), 0);
      newChildren.add(replacement);
      newChildren.add(expansionNode);
      newRoot = new Node("maxscore", expParams, newChildren, originalNode.getPosition());
    }
    rModel.cleanup();
    return newRoot;
  }

  public void beforeNode(Node object) throws Exception {
  }
}
