// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Transforms a #prms operator into a full expansion of the
 * PRM-S model. That means:
 *
 * Given `meg ryan war`, the output should be something like:
 *
 * #combine(
 * #feature:log (#combine:0=0.407:1=0.382:2=0.187 ( 
 *  #feature:dirichlet-raw(meg.cast) 
 *  #feature:dirichlet-raw(meg.team)  
 *  #feature:dirichlet-raw( meg.title) ) )
 * #feature:log (#combine:0=0.601:1=0.381:2=0.017 ( 
 *  #feature:dirichlet-raw(ryan.cast) 
 *  #feature:dirichlet-raw(ryan.team)
 *  #feature:dirichlet-raw(ryan.title) ) )
 * #feature:log (#combine:0=0.927:1=0.070:2=0.002 ( 
 *  #feature:dirichlet-raw(war.cast)
 *  #feature:dirichlet-raw(war.team)
 *  #feature:dirichlet-raw(war.title) )) 
 * )
 * 
 * Which looks verbose, but only because it's explicit:
 * count nodes are smoothed, but raw scores (not log-scores) are combined
 * linearly by a combine, then the linear combination is logged to avoid underflow,
 * then the log-sums are summed in log space. Simple.
 *
 * @author jykim, irmarc
 */
public class PRMS2Traversal extends Traversal {

  private int levels;
  String[] fieldList;
  Parameters availableFields;
  Parameters weights;
  Parameters prms = null;
  Parameters globals, queryParams;
  Retrieval retrieval;
  
    public PRMS2Traversal(Retrieval retrieval, Parameters qp) {
    levels = 0;
    queryParams = qp;
    this.retrieval = retrieval;
    try {
      availableFields = retrieval.getAvailableParts();
    } catch (Exception e) {
      throw new RuntimeException("Unable to get available parts");
    }
  }

  public static boolean isNeeded(Node root) {
    return (root.getOperator().equals("prms2"));
  }

  public void beforeNode(Node original) throws Exception {
    levels++;
  }

  public Node afterNode(Node original) throws Exception {
    levels--;
    if (levels > 0) {
      return original;
    } else if (original.getOperator().equals("prms2")) {
      globals = retrieval.getGlobalParameters();
      if (globals.containsKey("fields")) {
        getFieldsAndWeights(globals);
      } else {
        throw new IllegalArgumentException("PRMS expects fields to be specified in the parameters.");
      }
      String scorerType = globals.get("scorer", "dirichlet");

      List<Node> children = original.getInternalNodes();
      queryParams.set("numPotentials", children.size());
      ArrayList<Node> terms = new ArrayList<Node>();
      int j = 0;
      for (Node child : children) {
        ArrayList<Node> termFields = new ArrayList<Node>();
        NodeParameters nodeweights = new NodeParameters();
        int i = 0;
        double normalizer = 0.0; // sum_k of P(t|F_k)
        for (String field : fieldList) {
          String partName = "field." + field;
          if (!availableFields.containsKey(partName)) {
            continue;
          }

          NodeParameters par1 = new NodeParameters();
          par1.set("default", child.getDefaultParameter());
          par1.set("part", partName);
          Node termCount = new Node("counts", par1, new ArrayList(), 0);
	  double nw;
          if (weights != null && (weights.containsKey(field) || prms.containsKey("weight_default"))) {
	      if (weights.containsKey(field)) {
		  nw = weights.getDouble(field);
	      } else {
		  nw = prms.getDouble("weight_default");
	      }
          } else {
            NodeStatistics ns = retrieval.nodeStatistics(termCount);
            nw = (ns.nodeFrequency + 0.0) / ns.collectionLength; // P(t|F_j)
            normalizer += nw;
          }
	  nodeweights.set(Integer.toString(i), nw);
          Node termScore = new Node("feature", scorerType + "-raw");
          termScore.getNodeParameters().set("lengths", field);

	  // Following two sets are for delta-scoring
	  termScore.getNodeParameters().set("w", nw);
	  termScore.getNodeParameters().set("pIdx", j);

          termScore.addChild(termCount);
          termFields.add(termScore);
          i++;
        }

        // If we need to, apply the normalizer
        if (normalizer > 0.0) {
          for (i = 0; i < fieldList.length; i++) {
            String key = Integer.toString(i);
	    double normed = nodeweights.getDouble(key) / normalizer;
            nodeweights.set(key, normed);
	    termFields.get(i).getNodeParameters().set("w", normed);
            if (retrieval.getGlobalParameters().get("printWeights", false)) {
              double w = nodeweights.getDouble(key);
              if (w > 0.0) {
                System.err.printf("%s\t%s\t%f\n", child.getDefaultParameter(), fieldList[i], w);
              }
            }
          }
        }

        Node termFieldNodes = new Node("combine", nodeweights, termFields, 0);
        Node logScoreNode = new Node("feature", "log");
        logScoreNode.addChild(termFieldNodes);
        terms.add(logScoreNode);
	j++;
      }
      Node termNodes = new Node("combine", new NodeParameters(), terms, original.getPosition());
      termNodes.getNodeParameters().set("norm", false);
      return termNodes;
    } else {
      return original;
    }
  }

  private void getFieldsAndWeights(Parameters p) {

    if (!p.containsKey("fields")) {
      throw new IllegalArgumentException("parameter map should contain a 'fields' array.");
    }

    // Get out fields
    List<String> fields = (List<String>) p.getList("fields");
    fieldList = new String[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      fieldList[i] = fields.get(i);
    }

    // Look for optional weights
    if (p.containsKey("weights")) {
      weights = p.getMap("weights");
    } else {
      weights = null;
    }
  }
}
