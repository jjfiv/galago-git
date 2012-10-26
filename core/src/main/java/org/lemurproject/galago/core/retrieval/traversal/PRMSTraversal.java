// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.AggregateReader.IndexPartStatistics;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * Transforms a #prms operator into a full expansion of the
 * PRM-S model. That means:
 *
 * Given `meg ryan war`, the output should be like:
 *
 * #combine(
 * #combine:0=0.407:1=0.382:2=0.187 ( meg.cast  meg.team  meg.title )
 * #combine:0=0.601:1=0.381:2=0.017 ( ryan.cast  ryan.team  ryan.title )
 * #combine:0=0.927:1=0.070:2=0.002 ( war.cast  war.team  war.title ))
 *
 * @author jykim
 */
public class PRMSTraversal extends Traversal {

  private int levels;
  String[] fieldList;
  String[] weightList = null;
  Retrieval retrieval;
  
  public PRMSTraversal(Retrieval retrieval) {
    this.retrieval = retrieval;
    levels = 0;
  }

  public static boolean isNeeded(Node root) {
    return (root.getOperator().equals("prms"));
  }

  public void beforeNode(Node original) throws Exception {
    levels++;
  }

  public Node afterNode(Node original) throws Exception {
    levels--;
    if (levels > 0) {
      return original;
    } else if (original.getOperator().equals("prms")) {

      // Fetch the field list parameter from the query
      fieldList = ((List<String>)retrieval.getGlobalParameters().getAsList("fields")).toArray(new String[0]);
      try {
        weightList = original.getNodeParameters().getString("weights").split(",");
      } catch (java.lang.IllegalArgumentException e) {
      }
      // Get the field length
      Map<String, Long> fieldLengths = new HashMap<String, Long>();
      for (String field : fieldList) {
        IndexPartStatistics p = retrieval.getIndexPartStatistics("field." + field);
        fieldLengths.put(field, p.collectionLength);
      }

      List<Node> children = original.getInternalNodes();
      ArrayList<Node> terms = new ArrayList<Node>();
      for (Node child : children) {
        ArrayList<Node> termFields = new ArrayList<Node>();
        NodeParameters weights = new NodeParameters();
        int i = 0;
        for (String field : fieldList) {

          NodeParameters par1 = new NodeParameters();
          par1.set("default", child.getDefaultParameter());
          par1.set("part", "field." + field);
          Node termCount = new Node("counts", par1, new ArrayList(), 0);
          if (weightList != null) {
            weights.set(Integer.toString(i), weightList[i]);
          } else {
            long f_term_field = retrieval.getNodeStatistics(termCount).nodeFrequency;
            double f_term_field_prob = (double) f_term_field / fieldLengths.get(field);
            weights.set(Integer.toString(i), f_term_field_prob);
          }
          termFields.add(termCount);
          i++;
        }
        Node termFieldNodes = new Node("combine", weights, termFields, 0);
        terms.add(termFieldNodes);
      }
      Node termNodes = new Node("combine", new NodeParameters(), terms, original.getPosition());

      return termNodes;
    } else {
      return original;
    }
  }
}
