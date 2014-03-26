/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.retrieval.traversal;

import org.lemurproject.galago.core.index.stats.AggregateStatistic;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.GroupRetrieval;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.tupleflow.Parameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author sjh
 */
public class WeightedPhrankTraversal extends Traversal {

  private static final Logger logger = Logger.getLogger("WPHRANK");
  private Retrieval retrieval;
  private GroupRetrieval gRetrieval;
  private Parameters globalParams;
  private boolean verbose;
  private List<WPHRANKFeature> uniFeatures;
  private List<WPHRANKFeature> biFeatures;
  private List<WPHRANKFeature> triFeatures;

  public WeightedPhrankTraversal(Retrieval retrieval) throws Exception {
    if (retrieval instanceof GroupRetrieval) {
      gRetrieval = (GroupRetrieval) retrieval;
    }
    this.retrieval = retrieval;

    this.globalParams = retrieval.getGlobalParameters();

    verbose = globalParams.get("verboseWPHRANK", false);

    uniFeatures = new ArrayList();
    biFeatures = new ArrayList();
    triFeatures = new ArrayList();

    if (globalParams.isList("wphrankFeatures", Parameters.class)) {
      for (Parameters f : (List<Parameters>) globalParams.getList("wphrankFeatures")) {
        WPHRANKFeature wf = new WPHRANKFeature(f);
        if (wf.unigram) {
          uniFeatures.add(wf);
        }
        if (wf.bigram) {
          biFeatures.add(wf);
        }
        if (wf.trigram) {
          triFeatures.add(wf);
        }
      }

    } else {
      // default list of features: (using target collection only)
      uniFeatures.add(new WPHRANKFeature("1-const", WPHRANKFeatureType.CONST, 0.8, true));
      uniFeatures.add(new WPHRANKFeature("1-lntf", WPHRANKFeatureType.LOGTF, 0.0, true));
      uniFeatures.add(new WPHRANKFeature("1-lndf", WPHRANKFeatureType.LOGDF, 0.0, true));

      biFeatures.add(new WPHRANKFeature("2-const", WPHRANKFeatureType.CONST, 0.1, false));
      biFeatures.add(new WPHRANKFeature("2-lntf", WPHRANKFeatureType.LOGTF, 0.0, false));
      biFeatures.add(new WPHRANKFeature("2-lndf", WPHRANKFeatureType.LOGDF, 0.0, false));

      triFeatures.add(new WPHRANKFeature("2-const", WPHRANKFeatureType.CONST, 0.1, false));
      triFeatures.add(new WPHRANKFeature("2-lntf", WPHRANKFeatureType.LOGTF, 0.0, false));
      triFeatures.add(new WPHRANKFeature("2-lndf", WPHRANKFeatureType.LOGDF, 0.0, false));
    }
  }

  @Override
  public void beforeNode(Node original, Parameters queryParameters) throws Exception {
    // pass
  }

  @Override
  public Node afterNode(Node original, Parameters queryParameters) throws Exception {
    if (original.getOperator().equals("wphrank")) {

      NodeParameters np = original.getNodeParameters();

      List<Node> newChildren = new ArrayList();
      NodeParameters newWeights = new NodeParameters();

      for (Node child : original.getInternalNodes()) {
        if (child.getOperator().equals("text")) {

          String t1 = child.getDefaultParameter();
          double weight = computeWeight(child.getDefaultParameter(), np, queryParameters);

          newWeights.set("" + newChildren.size(), weight);
          newChildren.add(new Node("extents", t1));

        } else if (child.getOperator().equals("dep")) {

          if (child.numChildren() == 1) {
            String t1 = child.getDefaultParameter();
            double weight = computeWeight(child.getDefaultParameter(), np, queryParameters);

            newWeights.set("" + newChildren.size(), weight);
            newChildren.add(new Node("extents", t1));

          } else if (child.numChildren() == 2) {
            String t1 = child.getChild(0).getDefaultParameter();
            String t2 = child.getChild(1).getDefaultParameter();

            double weight = computeWeight(t1, t2, np, queryParameters);

            // #od:1
            Node od1 = new Node("ordered", new NodeParameters(1));
            od1.addChild(new Node("extents", t1));
            od1.addChild(new Node("extents", t2));

            // uw8
            Node uw8 = new Node("unordered", new NodeParameters(8));
            uw8.addChild(new Node("extents", t1));
            uw8.addChild(new Node("extents", t2));

            // duplicate the weights:
            newWeights.set("" + newChildren.size(), weight);
            newChildren.add(od1);

            newWeights.set("" + newChildren.size(), weight);
            newChildren.add(uw8);


          } else if (child.numChildren() == 3) {
            String t1 = child.getChild(0).getDefaultParameter();
            String t2 = child.getChild(1).getDefaultParameter();
            String t3 = child.getChild(2).getDefaultParameter();

            double weight = computeWeight(t1, t2, t3, np, queryParameters);

            // #od:1
            Node od1 = new Node("ordered", new NodeParameters(1));
            od1.addChild(new Node("extents", t1));
            od1.addChild(new Node("extents", t2));
            od1.addChild(new Node("extents", t3));

            // uw8
            Node uw8 = new Node("unordered", new NodeParameters(12));
            uw8.addChild(new Node("extents", t1));
            uw8.addChild(new Node("extents", t2));
            uw8.addChild(new Node("extents", t3));

            // duplicate the weights:
            newWeights.set("" + newChildren.size(), weight);
            newChildren.add(od1);

            newWeights.set("" + newChildren.size(), weight);
            newChildren.add(uw8);

          } else {
            logger.warning("Don't know what to do with child node:\n" + child.toString() + "\n -- Expected either 2 or 3 terms in dep.");
          }
        } else {
          logger.warning("Don't know what to do with child node:\n" + child.toString() + "\n -- Expected either #dep( #text:t() #text:t() ) or #text:t().");
        }
      }

      Node wphrank = new Node("combine", newWeights, newChildren, original.getPosition());
      return wphrank;
    }
    return original;
  }

  private double computeWeight(String term, NodeParameters np, Parameters queryParams) throws Exception {

    // we will probably need this for several features : 
    Node t = new Node("counts", term);
    t = TextPartAssigner.assignPart(t, queryParams, retrieval.getAvailableParts());

    // feature value store
    Map<WPHRANKFeature, Double> featureValues = new HashMap();

    // tf/df comes from the same object - can be used  twice
    Map<String, AggregateStatistic> localCache = new HashMap();

    // NOW : collect some feature values
    Node node;
    NodeStatistics featureStats;
    String cacheString;

    for (WPHRANKFeature f : uniFeatures) {
      switch (f.type) {
        case CONST:
          assert (!featureValues.containsKey(f));
          featureValues.put(f, 1.0);
          break;

        case LOGTF:
        case LOGNGRAMTF: // unigrams are the same
          assert (!featureValues.containsKey(f));

          // if the feature weight is 0 -- don't compute the feature
          if (queryParams.get(f.name, f.defLambda) == 0.0) {
            break;
          }

          node = t;
          if (!f.part.isEmpty()) {
            node = t.clone();
            node.getNodeParameters().set("part", f.part);
          }
          cacheString = node.toString() + "-" + f.group;

          if (localCache.containsKey(cacheString)) {
            featureStats = (NodeStatistics) localCache.get(cacheString);
          } else if (gRetrieval != null && !f.group.isEmpty()) {
            featureStats = gRetrieval.getNodeStatistics(node, f.group);
            localCache.put(cacheString, featureStats);
          } else {
            featureStats = this.retrieval.getNodeStatistics(node);
            localCache.put(cacheString, featureStats);
          }

          // only add the value if it occurs in the collection (log (0) = -Inf)
          if (featureStats.nodeFrequency != 0) {
            featureValues.put(f, Math.log(featureStats.nodeFrequency));
          }

          break;

        case LOGDF:
          assert (!featureValues.containsKey(f));
          // if the feature weight is 0 -- don't compute the feature
          if (queryParams.get(f.name, f.defLambda) == 0.0) {
            break;
          }

          node = t;
          if (!f.part.isEmpty()) {
            node = t.clone();
            node.getNodeParameters().set("part", f.part);
          }
          cacheString = node.toString() + "-" + f.group;

          if (localCache.containsKey(cacheString)) {
            featureStats = (NodeStatistics) localCache.get(cacheString);
          } else if (gRetrieval != null && !f.group.isEmpty()) {
            featureStats = gRetrieval.getNodeStatistics(node, f.group);
            localCache.put(cacheString, featureStats);
          } else {
            featureStats = this.retrieval.getNodeStatistics(node);
            localCache.put(cacheString, featureStats);
          }

          // only add the value if it occurs in the collection (log (0) = -Inf)
          if (featureStats.nodeDocumentCount != 0) {
            featureValues.put(f, Math.log(featureStats.nodeDocumentCount));
          }

          break;
      }
    }

    double weight = 0.0;
    for (WPHRANKFeature f : uniFeatures) {
      double lambda = np.get(f.name, queryParams.get(f.name, f.defLambda));
      if (featureValues.containsKey(f)) {
        weight += lambda * featureValues.get(f);
        if (verbose) {
          logger.info(String.format("%s -- feature:%s:%g * %g = %g", term, f.name, lambda, featureValues.get(f), lambda * featureValues.get(f)));
        }
      }
    }

    return weight;
  }

  private double computeWeight(String term1, String term2, NodeParameters np, Parameters queryParams) throws Exception {

    // prepare nodes (will be used several times)
    Node t1 = new Node("extents", term1);
    t1 = TextPartAssigner.assignPart(t1, queryParams, retrieval.getAvailableParts());
    Node t2 = new Node("extents", term2);
    t2 = TextPartAssigner.assignPart(t2, queryParams, retrieval.getAvailableParts());

    Node od1 = new Node("ordered");
    od1.getNodeParameters().set("default", 1);
    od1.addChild(t1);
    od1.addChild(t2);

    // feature value store
    Map<WPHRANKFeature, Double> featureValues = new HashMap();

    // tf/df comes from the same object - can be used  twice
    Map<String, AggregateStatistic> localCache = new HashMap();

    // NOW : collect some feature values
    Node node;
    NodeStatistics featureStats;
    String cacheString;

    for (WPHRANKFeature f : biFeatures) {
      switch (f.type) {
        case CONST:
          assert (!featureValues.containsKey(f));
          featureValues.put(f, 1.0);
          break;

        case LOGTF:
          assert (!featureValues.containsKey(f));
          // if the feature weight is 0 -- don't compute the feature
          if (queryParams.get(f.name, f.defLambda) == 0.0) {
            break;
          }

          node = od1;
          if (!f.part.isEmpty()) {
            node = od1.clone();
            node.getChild(0).getNodeParameters().set("part", f.part);
            node.getChild(1).getNodeParameters().set("part", f.part);
          }
          // f.group is "" or some particular group
          cacheString = node.toString() + "-" + f.group;

          // first check if we have already done this node.
          if (localCache.containsKey(cacheString)) {
            featureStats = (NodeStatistics) localCache.get(cacheString);
          } else if (gRetrieval != null && !f.group.isEmpty()) {
            featureStats = gRetrieval.getNodeStatistics(node, f.group);
            localCache.put(cacheString, featureStats);
          } else {
            featureStats = this.retrieval.getNodeStatistics(node);
            localCache.put(cacheString, featureStats);
          }

          // only add the value if it occurs in the collection (log (0) = -Inf)
          if (featureStats.nodeFrequency != 0) {
            featureValues.put(f, Math.log(featureStats.nodeFrequency));
          }

          break;

        case LOGDF:
          assert (!featureValues.containsKey(f));
          // if the feature weight is 0 -- don't compute the feature
          if (queryParams.get(f.name, f.defLambda) == 0.0) {
            break;
          }

          node = od1;
          if (!f.part.isEmpty()) {
            node = od1.clone();
            node.getChild(0).getNodeParameters().set("part", f.part);
            node.getChild(1).getNodeParameters().set("part", f.part);
          }
          cacheString = node.toString() + "-" + f.group;

          if (localCache.containsKey(cacheString)) {
            featureStats = (NodeStatistics) localCache.get(cacheString);
          } else if (gRetrieval != null && !f.group.isEmpty()) {
            featureStats = gRetrieval.getNodeStatistics(node, f.group);
            localCache.put(cacheString, featureStats);
          } else {
            featureStats = this.retrieval.getNodeStatistics(node);
            localCache.put(cacheString, featureStats);
          }

          // only add the value if it occurs in the collection (log (0) = -Inf)
          if (featureStats.nodeDocumentCount != 0) {
            featureValues.put(f, Math.log(featureStats.nodeDocumentCount));
          }

          break;

        case LOGNGRAMTF:
          assert (!featureValues.containsKey(f));
          // if the feature weight is 0 -- don't compute the feature
          if (queryParams.get(f.name, f.defLambda) == 0.0) {
            break;
          }

          node = new Node("counts", term1 + "~" + term2);
          if (!f.part.isEmpty()) {
            node.getNodeParameters().set("part", f.part);
          }
          // f.group is "" or some particular group
          cacheString = node.toString() + "-" + f.group;

          // first check if we have already done this node.
          if (localCache.containsKey(cacheString)) {
            featureStats = (NodeStatistics) localCache.get(cacheString);
          } else if (gRetrieval != null && !f.group.isEmpty()) {
            featureStats = gRetrieval.getNodeStatistics(node, f.group);
            localCache.put(cacheString, featureStats);
          } else {
            featureStats = this.retrieval.getNodeStatistics(node);
            localCache.put(cacheString, featureStats);
          }

          // only add the value if it occurs in the collection (log (0) = -Inf)
          if (featureStats.nodeFrequency != 0) {
            featureValues.put(f, Math.log(featureStats.nodeFrequency));
          }

          break;

      }
    }

    double weight = 0.0;
    for (WPHRANKFeature f : biFeatures) {
      double lambda = np.get(f.name, queryParams.get(f.name, f.defLambda));
      if (featureValues.containsKey(f)) {
        weight += lambda * featureValues.get(f);
        if (verbose) {
          logger.info(String.format("%s, %s -- feature:%s:%g * %g = %g", term1, term2, f.name, lambda, featureValues.get(f), lambda * featureValues.get(f)));
        }
      }
    }

    return weight;
  }

  private double computeWeight(String term1, String term2, String term3, NodeParameters np, Parameters queryParams) throws Exception {

    // prepare nodes (will be used several times)
    Node t1 = new Node("extents", term1);
    t1 = TextPartAssigner.assignPart(t1, queryParams, retrieval.getAvailableParts());
    Node t2 = new Node("extents", term2);
    t2 = TextPartAssigner.assignPart(t2, queryParams, retrieval.getAvailableParts());
    Node t3 = new Node("extents", term3);
    t3 = TextPartAssigner.assignPart(t3, queryParams, retrieval.getAvailableParts());

    Node od1 = new Node("ordered");
    od1.getNodeParameters().set("default", 1);
    od1.addChild(t1);
    od1.addChild(t2);
    od1.addChild(t3);

    // feature value store
    Map<WPHRANKFeature, Double> featureValues = new HashMap();

    // tf/df comes from the same object - can be used twice
    Map<String, AggregateStatistic> localCache = new HashMap();

    // NOW : collect some feature values
    Node node;
    NodeStatistics featureStats;
    String cacheString;

    for (WPHRANKFeature f : triFeatures) {
      switch (f.type) {
        case CONST:
          assert (!featureValues.containsKey(f));
          featureValues.put(f, 1.0);
          break;

        case LOGTF:
          assert (!featureValues.containsKey(f));
          // if the feature weight is 0 -- don't compute the feature
          if (queryParams.get(f.name, f.defLambda) == 0.0) {
            break;
          }

          node = od1;
          if (!f.part.isEmpty()) {
            node = od1.clone();
            node.getChild(0).getNodeParameters().set("part", f.part);
            node.getChild(1).getNodeParameters().set("part", f.part);
            node.getChild(2).getNodeParameters().set("part", f.part);
          }
          // f.group is "" or some particular group
          cacheString = node.toString() + "-" + f.group;

          // first check if we have already done this node.
          if (localCache.containsKey(cacheString)) {
            featureStats = (NodeStatistics) localCache.get(cacheString);
          } else if (gRetrieval != null && !f.group.isEmpty()) {
            featureStats = gRetrieval.getNodeStatistics(node, f.group);
            localCache.put(cacheString, featureStats);
          } else {
            featureStats = this.retrieval.getNodeStatistics(node);
            localCache.put(cacheString, featureStats);
          }

          // only add the value if it occurs in the collection (log (0) = -Inf)
          if (featureStats.nodeFrequency != 0) {
            featureValues.put(f, Math.log(featureStats.nodeFrequency));
          }

          break;

        case LOGDF:
          assert (!featureValues.containsKey(f));
          // if the feature weight is 0 -- don't compute the feature
          if (queryParams.get(f.name, f.defLambda) == 0.0) {
            break;
          }

          node = od1;
          if (!f.part.isEmpty()) {
            node = od1.clone();
            node.getChild(0).getNodeParameters().set("part", f.part);
            node.getChild(1).getNodeParameters().set("part", f.part);
            node.getChild(2).getNodeParameters().set("part", f.part);
          }
          cacheString = node.toString() + "-" + f.group;

          if (localCache.containsKey(cacheString)) {
            featureStats = (NodeStatistics) localCache.get(cacheString);
          } else if (gRetrieval != null && !f.group.isEmpty()) {
            featureStats = gRetrieval.getNodeStatistics(node, f.group);
            localCache.put(cacheString, featureStats);
          } else {
            featureStats = this.retrieval.getNodeStatistics(node);
            localCache.put(cacheString, featureStats);
          }

          // only add the value if it occurs in the collection (log (0) = -Inf)
          if (featureStats.nodeDocumentCount != 0) {
            featureValues.put(f, Math.log(featureStats.nodeDocumentCount));
          }

          break;

        case LOGNGRAMTF:
          assert (!featureValues.containsKey(f));
          // if the feature weight is 0 -- don't compute the feature
          if (queryParams.get(f.name, f.defLambda) == 0.0) {
            break;
          }

          node = new Node("counts", term1 + "~" + term2 + "~" + term3);
          if (!f.part.isEmpty()) {
            node.getNodeParameters().set("part", f.part);
          }
          // f.group is "" or some particular group
          cacheString = node.toString() + "-" + f.group;

          // first check if we have already done this node.
          if (localCache.containsKey(cacheString)) {
            featureStats = (NodeStatistics) localCache.get(cacheString);
          } else if (gRetrieval != null && !f.group.isEmpty()) {
            featureStats = gRetrieval.getNodeStatistics(node, f.group);
            localCache.put(cacheString, featureStats);
          } else {
            featureStats = this.retrieval.getNodeStatistics(node);
            localCache.put(cacheString, featureStats);
          }

          // only add the value if it occurs in the collection (log (0) = -Inf)
          if (featureStats.nodeFrequency != 0) {
            featureValues.put(f, Math.log(featureStats.nodeFrequency));
          }

          break;
      }
    }

    double weight = 0.0;
    for (WPHRANKFeature f : triFeatures) {
      double lambda = np.get(f.name, queryParams.get(f.name, f.defLambda));
      if (featureValues.containsKey(f)) {
        weight += lambda * featureValues.get(f);
        if (verbose) {
          logger.info(String.format("%s, %s, %s -- feature:%s:%g * %g = %g", term1, term2, term3, f.name, lambda, featureValues.get(f), lambda * featureValues.get(f)));
        }
      }
    }

    return weight;
  }

  public static enum WPHRANKFeatureType {

    LOGTF, LOGDF, CONST, LOGNGRAMTF;
  }

  /*
   * Features for WSDM: 
   *  name : "1-gram" 
   *  tfFeature : [true | false] :: asserts [ tf or df ], (tf default)
   *  group : "retrievalGroupName" :: missing or empty = default 
   *  part : "retrievalPartName" :: missing or empty = default
   *  unigram : true|false :: can be used on unigrams
   *  bigram : true|false :: can be used on bigrams
   */
  public static class WPHRANKFeature {

    public String name;
    public WPHRANKFeatureType type; // [tf | df | const] -- others may be supported later
    public String group;
    public String part;
    public double defLambda;
    // mutually exclusive unigram/bigram
    public boolean unigram;
    public boolean bigram;
    public boolean trigram;

    public WPHRANKFeature(Parameters p) {
      this.name = p.getString("name");
      this.type = WPHRANKFeatureType.valueOf(p.get("type", "logtf").toUpperCase());
      this.defLambda = p.get("lambda", 1.0);
      this.group = p.get("group", "");
      this.part = p.get("part", "");
      this.unigram = p.get("unigram", true);
      this.bigram = p.get("bigram", !unigram);
      this.trigram = p.get("trigram", !unigram && !bigram);
    }

    /*
     * Constructor to allow default list of features
     */
    public WPHRANKFeature(String name, WPHRANKFeatureType type, double defLambda, boolean unigram) {
      this.name = name;
      this.type = type;
      this.defLambda = defLambda;
      this.group = "";
      this.part = "";
      this.unigram = unigram;
      this.bigram = !unigram;
      this.trigram = !unigram;
    }
  }
}
