// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Parameters.Type;

/**
 * Transforms a #prms operator into a full expansion of the PRM-S model. That
 * means:
 *
 * Given `meg ryan war`, and fields `cast team title` the output should be
 * something like:
 *
 * #combine( #wsum:0=0.407:1=0.382:2=0.187 ( #dirichlet(meg.cast)
 * #dirichlet(meg.team) #dirichlet( meg.title) )
 * #wsum:0=0.601:1=0.381:2=0.017 ( #dirichlet(ryan.cast)
 * #dirichlet(ryan.team) #dirichlet(ryan.title) )
 * #wsum:0=0.927:1=0.070:2=0.002 ( #dirichlet(war.cast)
 * #dirichlet(war.team) #dirichlet(war.title) ) )
 *
 * @author jykim, irmarc, sjh
 */
public class PRMS2Traversal extends Traversal {

  private Retrieval retrieval;
  private Parameters globals;
  private List<String> defaultFields;
  private Parameters defaultWeights;

  public PRMS2Traversal(Retrieval retrieval) {
    this.retrieval = retrieval;
    this.globals = retrieval.getGlobalParameters();

    // get field list
    if (globals.isList("fields", Type.STRING)) {
      this.defaultFields = (List<String>) globals.getAsList("fields");
    } else {
      this.defaultFields = null;
    }

    // get field weights
    if (globals.isMap("weights")) {
      this.defaultWeights = globals.getMap("weights");
    } else {
      this.defaultWeights = null;
    }
  }

  @Override
  public void beforeNode(Node original, Parameters queryParameters) throws Exception {
  }

  @Override
  public Node afterNode(Node original, Parameters queryParameters) throws Exception {
    if (original.getOperator().equals("prms2") || original.getOperator().equals("prms")) {

      String scorerType = queryParameters.get("scorer", globals.get("scorer", "dirichlet"));
      
      List<String> fields = queryParameters.isList("fields", Type.STRING) ? queryParameters.getList("fields") : defaultFields;
      Parameters weights = queryParameters.isMap("weights") ? queryParameters.getMap("weights") : defaultWeights;

      List<String> terms = getTextTerms(original.getInternalNodes());

      // collect some information about fields
      Map<String, FieldStatistics> fieldStats = new HashMap();
      Map<String, Node> fieldLenNodes = new HashMap();

      for (String field : fields) {
        Node fieldLen = StructuredQuery.parse("#lengths:" + field + ":part=lengths()");
        FieldStatistics fieldStat = retrieval.getCollectionStatistics(fieldLen);
        fieldStats.put(field, fieldStat);
        fieldLenNodes.put(field, fieldLen);
      }

      ArrayList<Node> termNodes = new ArrayList<Node>();

      // for each term - generate a wsum node combining field level evidence
      for (String term : terms) {

        ArrayList<Node> termFields = new ArrayList<Node>();
        NodeParameters nodeweights = new NodeParameters();
        int i = 0;
        double normalizer = 0.0; // sum_k of P(t|F_k)

        for (String field : fields) {
          Node termFieldCounts, termExtents;

          // if we have access to the correct field-part:
          if (this.retrieval.getAvailableParts().containsKey("field." + field)) {
            NodeParameters par1 = new NodeParameters();
            par1.set("default", term);
            par1.set("part", "field." + field);
            termFieldCounts = new Node("counts", par1, new ArrayList());

          } else {
            // otherwise use an #inside op
            NodeParameters par1 = new NodeParameters();
            par1.set("default", term);
            termExtents = new Node("extents", par1, new ArrayList());
            termExtents = TextPartAssigner.assignPart(termExtents, globals, this.retrieval.getAvailableParts());

            termFieldCounts = new Node("inside");
            termFieldCounts.addChild(StructuredQuery.parse("#extents:part=extents:" + field + "()"));
            termFieldCounts.addChild(termExtents);
          }

          // if weights is set - there is some attempt to weight the fields manually
          //  - if this particular field is not weighted, use 1.0
          if (weights != null) {
            if (weights.containsKey(field)) {
              nodeweights.set(Integer.toString(i), weights.getDouble(field));
            } else {
              nodeweights.set(Integer.toString(i), 1.0);
            }

          } else {
            // otherwise there are no weights
            //  - weight fields according to the probability of this term coming from this field

            NodeStatistics ns = retrieval.getNodeStatistics(termFieldCounts);
            double fieldprob = (double) ns.nodeFrequency / (double) fieldStats.get(field).collectionLength; // P(t|F_j)
            nodeweights.set(Integer.toString(i), fieldprob);

            normalizer += fieldprob;
          }

          Node termScore = new Node(scorerType);
          termScore.getNodeParameters().set("lengths", field);
          termScore.addChild(fieldLenNodes.get(field).clone());
          termScore.addChild(termFieldCounts);
          termFields.add(termScore);
          i++;
        }

        // If we need to, apply the normalizer
        if (normalizer > 0.0) {
          for (i = 0; i < fields.size(); i++) {
            String key = Integer.toString(i);
            nodeweights.set(key, nodeweights.getDouble(key) / normalizer);
          }
        }

        Node termFieldNodes = new Node("wsum", nodeweights, termFields, 0);
        termNodes.add(termFieldNodes);
      }
      Node root = new Node("combine", new NodeParameters(), termNodes, original.getPosition());
      root.getNodeParameters().set("norm", false);
      return root;
    } else {
      return original;
    }
  }

  private List<String> getTextTerms(List<Node> nodes) throws IOException {
    ArrayList<String> terms = new ArrayList();
    for (Node n : nodes) {
      if (n.getOperator().equals("text")) {
        terms.add(n.getDefaultParameter());
      } else {
        Logger.getLogger("PRMSTraversal").info("Could not extract term from child node: " + n.toString());
        throw new IOException("PRMSTraversal could not extract term from child node: " + n.toString());
      }
    }
    return terms;
  }
}
