// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * 
 * This is an extension of the #prms model to support field priors.
 * 
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
 *
 * @author jykim, irmarc, jdalton
 */
public class PRMS3Traversal extends Traversal {

    private int levels;
    private final List<String> fields;
    private final Parameters availableFields;
    private final Map<String,Double> fieldWeights = new HashMap<String, Double>();
    
    private final Retrieval retrieval;
    private final Parameters globals;
    private final Parameters queryParameters;

    private double fieldWeightAlpha = 1;
    
    public PRMS3Traversal(Retrieval retrieval, Parameters queryParameters) {
        levels = 0;
        this.retrieval = retrieval;
        this.globals = retrieval.getGlobalParameters();
        this.queryParameters = queryParameters;
        try {
            availableFields = retrieval.getAvailableParts();
        } catch (Exception e) {
            throw new RuntimeException("Unable to get available parts");
        }
        
        if (!queryParameters.containsKey("fields")) {
            throw new IllegalArgumentException("Query parameter map should contain a 'fields' array.");
        } else {
            fields = (List<String>) queryParameters.getAsList("fields");
        }
        
        // check validity of fields.
        for (String field : fields) {
            String partName = "field." + field;
            if (!availableFields.containsKey(partName)) {
                throw new IllegalArgumentException("PRMS part for field: " + field + " is unavailable.  Please check field parameters.");
            }
        }
        
        // Look for optional field weights
        if (queryParameters.containsKey("weights")) {
            Parameters fieldParams = queryParameters.getMap("weights");
            // if specified, they need to match the fields.
            for (String field : fields) {
                if (!fieldParams.containsKey(field)) {
                    throw new IllegalArgumentException("Mismatch between field weights and declared fields.  Missing weight for field: " + field);
                }
                double weight = fieldParams.getDouble(field);
                fieldWeights.put(field, weight);
            }
        } else {
            // assume a uniform field weighting
            for (String field : fields) {
                fieldWeights.put(field, 1.0d);
            }
        }
        
        if (queryParameters.containsKey("fieldWeightAlpha")) {
            fieldWeightAlpha = queryParameters.getDouble("fieldWeightAlpha");
        }
        
    }

    public static boolean isNeeded(Node root) {
        return (root.getOperator().equals("prms3"));
    }

    public void beforeNode(Node original) throws Exception {
        levels++;
    }

    public Node afterNode(Node original) throws Exception {
        levels--;
        if (levels > 0) {
            return original;
        } else if (original.getOperator().equals("prms3")) {
            
            String scorerType = globals.get("scorer", "dirichlet");

            ArrayList<Node> terms = new ArrayList<Node>();
            
            List<Node> children = original.getInternalNodes();
            for (Node child : children) {
                
                ArrayList<Node> termFields = new ArrayList<Node>();
                NodeParameters nodeweights = new NodeParameters();
                int i = 0;
                double normalizer = 0.0; // sum_k of P(t|F_k)
                for (String field : fields) {
                    
                    // dummy up node parameter and statistic objects.
                    NodeParameters par1 = new NodeParameters();
                    par1.set("default", child.getDefaultParameter());
                    par1.set("part", "field." + field);
                    Node termCount = new Node("counts", par1, new ArrayList(), 0);
                    
                    // lookup collection statistics for each term query
                    // and calculate the field probability.
                    NodeStatistics ns = retrieval.nodeStatistics(termCount);
                    double fieldProb = (ns.nodeFrequency + 0.0) / ns.collectionLength; // P(t|F_j)
                    
                    // linearly interpolate field probability with field prior weight.                    
                    double fieldWeight = (1-fieldWeightAlpha) * fieldWeights.get(field) + (fieldWeightAlpha * fieldProb);
                    
                    nodeweights.set(Integer.toString(i), fieldWeight);
                    normalizer += fieldWeight;

                    Node termScore = new Node("feature", scorerType + "-raw");
                    termScore.getNodeParameters().set("lengths", field);
                    termScore.addChild(termCount);
                    termFields.add(termScore);
                    i++;
                }

                // If we need to, apply the normalizer
                if (normalizer > 0.0) {
                    int j=0;
                    for (String field : fields) {
                        String key = Integer.toString(j);
                        nodeweights.set(key, nodeweights.getDouble(key) / normalizer);
                        if (retrieval.getGlobalParameters().get("printWeights", false)) {
                            double w = nodeweights.getDouble(key);
                            if (w > 0.0) {
                                System.err.printf("%s\t%s\t%f\n", child.getDefaultParameter(), field, w);
                            }
                        }
                        j++;
                    }
                }

                Node termFieldNodes = new Node("combine", nodeweights, termFields, 0);
                Node logScoreNode = new Node("feature", "log");
                logScoreNode.addChild(termFieldNodes);
                terms.add(logScoreNode);
            }
            Node termNodes = new Node("combine", new NodeParameters(), terms, original.getPosition());
            termNodes.getNodeParameters().set("norm", false);
            return termNodes;
        } else {
            return original;
        }
    }

}
