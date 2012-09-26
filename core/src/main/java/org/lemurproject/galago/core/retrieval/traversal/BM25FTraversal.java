// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics2;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Transforms a #bm25f operator into a full expansion of the
 * BM25F model. That means:
 *
 * Given `meg ryan war`, the output should be like:
 *
 * #bm25fcomb:K=0.5(
 * #combine:0=0.407:1=0.382:2=0.187 ( #feature:bm25f:b=0.1(meg.cast)  #feature:bm25f:b=0.2(meg.team)  #feature:bm25f:b=0.3(meg.title) )
 * #idf(meg)
 * #combine:0=0.407:1=0.382:2=0.187 ( #feature:bm25f:b=0.1(ryan.cast)  #feature:bm25f:b=0.2(ryan.team)  #feature:bm25f:b=0.3(ryan.title) )
 * #idf(ryan)
 * #combine:0=0.407:1=0.382:2=0.187 ( #feature:bm25f:b=0.1(war.cast)  #feature:bm25f:b=0.2(war.team)  #feature:bm25f:b=0.3(war.title) )
 * #idf(war) )
 *
 * Except the weights involved should be tuned and not made up. Weights are:
 *
 * - b_f : A 'b' that is tuned for that field. It is NOT dependent to a query - only to the field. (attached to feature nodes)
 * - W_f : A weight for each field, that is multiplied against every term for that field, making the total term weight.
 * - K : A field/query independent tunable parameter. Used for smoothing term scores.
 *
 *
 * @author irmarc
 */
public class BM25FTraversal extends Traversal {

  private int levels;
  List<String> fieldList;
  Parameters availableFields, weights, queryParams;
  Retrieval retrieval;
  
  public BM25FTraversal(Retrieval retrieval, Parameters queryParams) {
    levels = 0;
    this.retrieval = retrieval;
    this.queryParams = queryParams;
    Parameters globals = retrieval.getGlobalParameters();
    weights = globals.containsKey("bm25f") ? globals.getMap("bm25f") : new Parameters();
    fieldList = globals.getAsList("fields");
    try {
	availableFields = retrieval.getAvailableParts();
    } catch (Exception e) {
	throw new RuntimeException(e);
    }
  }

  public static boolean isNeeded(Node root) {
    return (root.getOperator().equals("bm25f"));
  }

  public void beforeNode(Node original) throws Exception {
    levels++;
  }

  public Node afterNode(Node original) throws Exception {
    levels--;
    if (levels == 0 && original.getOperator().equals("bm25f")) {
      // Create the replacing root
      NodeParameters rootP = new NodeParameters();
      rootP.set("K", weights.get("K", 0.5));
      Parameters cumulativeWeights = weights.containsKey("weights") ? weights.getMap("weights") : new Parameters();
      Parameters smoothing = weights.containsKey("smoothing") ? weights.getMap("smoothing") : new Parameters();
      Node newRoot = new Node("bm25fcomb", rootP);
      newRoot.getNodeParameters().set("norm", false);
      // Now generate the field-based subtrees for all extent/count nodes
      // NOTE : THIS IS BROKEN. IT WON'T RECOGNIZE WINDOW COUNT NODES, BUT IT SHOULD
      List<Node> children = original.getInternalNodes();
      queryParams.set("numPotentials", children.size());
      for (int i = 0; i < children.size(); i++) {
        Node termNode = children.get(i);
        double idf = getIDF(termNode);
        Node termCombiner = createFieldsOfTerm(termNode, smoothing, cumulativeWeights, i, weights.get("K", 0.5),
                                               idf);
        newRoot.addChild(termCombiner);
        newRoot.getNodeParameters().set("idf"+i, idf);
      }
      return newRoot;
    } else {
      return original;
    }
  }

  private double getIDF(Node termNode) throws Exception {
    // get the global document count:
    CollectionStatistics2 cs = retrieval.collectionStatistics("#lengths:part=lengths()");
    double documentCount = cs.documentCount;
    
    // get the number of documents this term occurs in:
    NodeStatistics ns = retrieval.nodeStatistics(termNode.toString());
    long df = ns.nodeDocumentCount;

    // compute idf and return
    double idf = Math.log(documentCount / (df + 0.5));
    return idf;
  }

    private Node createFieldsOfTerm(Node termNode, Parameters smoothingWeights,
				    Parameters cumulativeWeights, int pos, double K, double idf) throws Exception {
    String term = termNode.getDefaultParameter();

    // Use a straight weighting - no weight normalization
    Node combiner = new Node("combine", new ArrayList<Node>());
    combiner.getNodeParameters().set("norm", false);

    for (String field : fieldList) {
	// Make sure the part exists
	String partName = "field." + field;
	if (!availableFields.containsKey(partName)) continue;
      // Actual count node
      NodeParameters np = new NodeParameters();
      np.set("default", term);
      np.set("part", partName);
      Node fieldTermNode = new Node("extents", np);

      // Now wrap it in the scorer
      np = new NodeParameters();
      np.set("b", smoothingWeights.get(field, weights.get("smoothing_default", 0.5)));
      np.set("default", "bm25f");
      np.set("lengths", field);
      np.set("pIdx", pos);
      np.set("K", K);
      np.set("idf", idf);
      np.set("w", cumulativeWeights.get(field, weights.get("weight_default", 0.5)));
      Node fieldScoreNode = new Node("feature", np);
      fieldScoreNode.addChild(fieldTermNode);
      combiner.getNodeParameters().set(Integer.toString(combiner.getInternalNodes().size()),
				       cumulativeWeights.get(field, weights.get("weight_default", 0.5)));
      combiner.addChild(fieldScoreNode);
    }

    return combiner;
  }
}
