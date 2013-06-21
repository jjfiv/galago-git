// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.GroupRetrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.MalformedQueryException;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Weighted Sequential Dependency Model model is structurally similar to the
 * Sequential Dependency Model, however node weights are the linear combination
 * of some node features.
 * 
 *  (based on bendersky 2010)
 *
 * In particular the weight for a node "term" is determined to be:
 * weight("term") = unigram_constant + cf(term) * unigram_cf_lambda + df(term)
 * *unigram_df_lambda + wiki_title_cf(term) * unigram_wiki_lambda
 *
 * bigram weights are determined using a similar method.
 *
 * The constant and lambda values are tunable parameters of the model.
 *
 * @author sjh
 */
public class WeightedSequentialDependenceTraversal extends Traversal {

  FieldStatistics collStats;
  FieldStatistics wikiStats;
  FieldStatistics msnStats;
  IndexPartStatistics wikiPartStats;
  IndexPartStatistics msnPartStats;
  IndexPartStatistics google1PartStats;
  IndexPartStatistics google2PartStats;
  private Retrieval retrieval;
  private GroupRetrieval gRetrieval;
  private Parameters globalParams;
  private boolean defLogFrequencies;
  private boolean defProbFrequencies;
  private boolean defCombNorm;
  private boolean verbose;
  private Map<String, Double> defUnigramWeights;
  private Map<String, Double> defBigramWeights;
  private String wikiGroup;
  private String msnGroup;
  private String wikiTitlePart;
  private String goog1Part;
  private String goog2Part;
  private String msnPart;

  public WeightedSequentialDependenceTraversal(Retrieval retrieval) throws Exception {
    if (retrieval instanceof GroupRetrieval) {
      gRetrieval = (GroupRetrieval) retrieval;
    }
    this.retrieval = retrieval;

    this.globalParams = retrieval.getGlobalParameters();

    verbose = globalParams.get("verboseWSDM", false);

    defLogFrequencies = globalParams.get("logFreq", true);
    defProbFrequencies = globalParams.get("probFreq", false);
    defCombNorm = globalParams.get("norm", false);

    assert (!(defLogFrequencies && defProbFrequencies)) : "WeightedSequentialDependenceTraversal can use either 'log' or 'prob' frequencies, not both.";

    if (gRetrieval != null) {
      wikiGroup = globalParams.get("wikiTitleIndexGroup", (String) null);
      msnGroup = globalParams.get("msnIndexGroup", (String) null);
    }

    // constructed by build-special-coll-background
    goog1Part = globalParams.get("googleUnigramPart", (String) null);
    goog2Part = globalParams.get("googleBigramPart", (String) null);

    // constructed by build-special-coll-background
    wikiTitlePart = globalParams.get("wikiTitlePart", (String) null);

    // constructed by build-special-coll-background
    msnPart = globalParams.get("msnPart", (String) null);

    // parameters from local index
    defUnigramWeights = new HashMap();
    defUnigramWeights.put("uni-const", globalParams.get("uni-const", 0.8));
    defUnigramWeights.put("uni-tf", globalParams.get("uni-tf", 0.0));
    defUnigramWeights.put("uni-df", globalParams.get("uni-df", 0.0));

    defBigramWeights = new HashMap();
    defBigramWeights.put("bi-const", globalParams.get("bi-const", 0.1));
    defBigramWeights.put("bi-tf", globalParams.get("bi-tf", 0.0));
    defBigramWeights.put("bi-df", globalParams.get("bi-df", 0.0));
    defBigramWeights.put("bi-pmi", globalParams.get("bi-pmi", 0.0));

    // parameters from various special information sources
    if (goog1Part != null) {
      defUnigramWeights.put("uni-google-tf", globalParams.get("uni-google-tf", 0.0));
    }
    if (goog2Part != null) {
      defBigramWeights.put("bi-google-tf", globalParams.get("bi-google-tf", 0.0));
      defBigramWeights.put("bi-google-pmi", globalParams.get("bi-google-pmi", 0.0));
    }
    if (msnPart != null) {
      defUnigramWeights.put("uni-msn-e", globalParams.get("uni-msn-e", 0.0));
      defBigramWeights.put("bi-msn-e", globalParams.get("bi-msn-e", 0.0));
    }

    if (msnGroup != null) {
      defUnigramWeights.put("uni-msn-tf", globalParams.get("uni-msn-tf", 0.0));
      defBigramWeights.put("bi-msn-tf", globalParams.get("bi-msn-tf", 0.0));
      defBigramWeights.put("bi-msn-pmi", globalParams.get("bi-msn-pmi", 0.0));
    }

    if (wikiTitlePart != null) {
      defUnigramWeights.put("uni-wt-e", globalParams.get("uni-wt-e", 0.0));
      defBigramWeights.put("bi-wt-e", globalParams.get("bi-wt-e", 0.0));
    }

    if (wikiGroup != null) {
      defUnigramWeights.put("uni-wt-tf", globalParams.get("uni-wt-tf", 0.0));
      defBigramWeights.put("bi-wt-tf", globalParams.get("bi-wt-tf", 0.0));
      defBigramWeights.put("bi-wt-pmi", globalParams.get("bi-wt-pmi", 0.0));
    }


    // collect some stats for normalization
    if (defProbFrequencies) {
      collStats = retrieval.getCollectionStatistics("#lengths:document:part=lengths()");

      if (wikiGroup != null) {
        wikiStats = gRetrieval.getCollectionStatistics("#lengths:document:part=lengths()", wikiGroup);
      }
      if (msnGroup != null) {
        msnStats = gRetrieval.getCollectionStatistics("#lengths:document:part=lengths()", msnGroup);
      }
      if (goog1Part != null) {
        google1PartStats = retrieval.getIndexPartStatistics(goog1Part);
      }
      if (goog2Part != null) {
        google2PartStats = retrieval.getIndexPartStatistics(goog2Part);
      }
      if (msnPart != null) {
        msnPartStats = retrieval.getIndexPartStatistics(msnPart);
      }
      if (wikiTitlePart != null) {
        wikiPartStats = retrieval.getIndexPartStatistics(wikiTitlePart);
      }
    }
  }

  @Override
  public void beforeNode(Node original, Parameters queryParameters) throws Exception {
  }

  @Override
  public Node afterNode(Node original, Parameters queryParams) throws Exception {
    if (original.getOperator().equals("wsdm")) {

      NodeParameters np = original.getNodeParameters();

      // First check format - should only contain text node children
      List<Node> children = original.getInternalNodes();
      for (Node child : children) {
        if (child.getOperator().equals("text") == false) {
          throw new MalformedQueryException("wsdm operator requires text-only children");
        }
      }

      // formatting is ok - now reassemble
      ArrayList<Node> newChildren = new ArrayList();
      NodeParameters newWeights = new NodeParameters();
      // i don't want normalization -- even though michael used some.
      newWeights.set("norm", defCombNorm);


      for (Node child : children) {
        String term = child.getDefaultParameter();

        double weight = computeWeight(term, np, queryParams);
        newWeights.set(Integer.toString(newChildren.size()), weight);
        newChildren.add(child.clone());
      }

      for (int i = 0; i < (children.size() - 1); i++) {
        ArrayList<Node> pair = new ArrayList();
        pair.add(new Node("extents", children.get(i).getDefaultParameter()));
        pair.add(new Node("extents", children.get(i + 1).getDefaultParameter()));

        double weight = computeWeight(pair.get(0).getDefaultParameter(), pair.get(1).getDefaultParameter(), np, queryParams);

        newWeights.set(Integer.toString(newChildren.size()), weight);
        newChildren.add(new Node("od", new NodeParameters(1), Node.cloneNodeList(pair)));

        newWeights.set(Integer.toString(newChildren.size()), weight);
        newChildren.add(new Node("uw", new NodeParameters(8), Node.cloneNodeList(pair)));
      }

      Node wsdm = new Node("combine", newWeights, newChildren, original.getPosition());

      if (verbose) {
        System.err.println(wsdm.toPrettyString());
      }

      return wsdm;
    } else {
      return original;
    }
  }

  private double computeWeight(String term, NodeParameters np, Parameters queryParams) throws Exception {
    
    // override default weights
    Map<String, Double> featureWeights = new HashMap(defUnigramWeights.size());
    for(String key : defUnigramWeights.keySet()){
      featureWeights.put(key, np.get(key, queryParams.get(key, this.defUnigramWeights.get(key))));
    }
    
    // start collecting feature values (should replace this spagetti code with feature-getters)
    Map<String, Double> featureValues = new HashMap(defUnigramWeights.size());
    featureValues.put("uni-const", 1.0);

    // prepare node (will be used several times)
    Node t = new Node("counts", term);
    t = TextPartAssigner.assignPart(t, queryParams, retrieval.getAvailableParts());

    if ((featureWeights.get("uni-tf") != 0.0) || (featureWeights.get("uni-df") != 0.0)) {
      NodeStatistics stats = retrieval.getNodeStatistics(t);

      if (stats.nodeFrequency > 0) {
        if (defProbFrequencies) {
          featureValues.put("uni-tf", (stats.nodeFrequency / (double) this.collStats.collectionLength));
          featureValues.put("uni-df", (stats.nodeDocumentCount / (double) this.collStats.documentCount));
        } else if (defLogFrequencies) {
          featureValues.put("uni-tf", Math.log(stats.nodeFrequency));
          featureValues.put("uni-df", Math.log(stats.nodeDocumentCount));
        } else {
          featureValues.put("uni-tf", (double) stats.nodeFrequency);
          featureValues.put("uni-df", (double) stats.nodeDocumentCount);
        }
      } else {
        featureValues.put("uni-tf", 0.0);
        featureValues.put("uni-df", 0.0);
      }
    }

    // google-uni-grams
    if (goog1Part != null && featureWeights.get("uni-google-tf") != 0.0) {
      t.getNodeParameters().set("part", goog1Part);
      NodeStatistics stats = retrieval.getNodeStatistics(t);

      if (stats.nodeFrequency > 0) {
        if (defProbFrequencies) {
          featureValues.put("uni-google-tf", (stats.nodeFrequency / (double) this.google1PartStats.collectionLength));
        } else if (defLogFrequencies) {
          featureValues.put("uni-google-tf", Math.log(stats.nodeFrequency));
        } else {
          featureValues.put("uni-google-tf", (double) stats.nodeFrequency);
        }
      } else {
        featureValues.put("uni-google-tf", 0.0);
      }
    }

    // wiki-title-existences
    if (wikiTitlePart != null && featureWeights.get("uni-wt-e") != 0.0) {
      t.getNodeParameters().set("part", wikiTitlePart);
      NodeStatistics stats = retrieval.getNodeStatistics(t);

      if (stats.nodeFrequency > 0) {
        featureValues.put("uni-wt-e", 1.0);
      } else {
        featureValues.put("uni-wt-e", 0.0);
      }
    }

    // wiki-title-existences
    if (msnPart != null && featureWeights.get("uni-msn-e") != 0.0) {
      t.getNodeParameters().set("part", msnPart);
      NodeStatistics stats = retrieval.getNodeStatistics(t);

      if (stats.nodeFrequency > 0) {
        featureValues.put("uni-msn-e", 1.0);
      } else {
        featureValues.put("uni-msn-e", 0.0);
      }
    }


    // wiki group
    if (wikiGroup != null && featureWeights.get("uni-wt-tf") != 0.0) {
      Node wt = new Node("counts", term);
      wt = TextPartAssigner.assignPart(wt, queryParams, gRetrieval.getAvailableParts(wikiGroup));
      NodeStatistics stats = gRetrieval.getNodeStatistics(wt, wikiGroup);

      if (stats.nodeFrequency > 0) {
        if (defProbFrequencies) {
          featureValues.put("uni-wt-tf", (stats.nodeFrequency / (double) this.wikiStats.collectionLength));
        } else if (defLogFrequencies) {
          featureValues.put("uni-wt-tf", Math.log(stats.nodeFrequency));
        } else {
          featureValues.put("uni-wt-tf", (double) stats.nodeFrequency);
        }
      } else {
        featureValues.put("uni-wt-tf", 0.0);
      }
    }

    // msn group
    if (msnGroup != null && featureWeights.get("uni-msn-tf") != 0.0) {
      Node mt = new Node("counts", term);
      mt = TextPartAssigner.assignPart(mt, queryParams, gRetrieval.getAvailableParts(wikiGroup));
      NodeStatistics stats = gRetrieval.getNodeStatistics(mt, msnGroup);

      if (stats.nodeFrequency > 0) {
        if (defProbFrequencies) {
          featureValues.put("uni-msn-tf", (stats.nodeFrequency / (double) this.msnStats.collectionLength));
        } else if (defLogFrequencies) {
          featureValues.put("uni-msn-tf", Math.log(stats.nodeFrequency));
        } else {
          featureValues.put("uni-msn-tf", (double) stats.nodeFrequency);
        }
      } else {
        featureValues.put("uni-msn-tf", 0.0);
      }
    }

    double weight = 0.0;

    for (String feature : featureValues.keySet()) {
      weight += featureValues.get(feature) * featureWeights.get(feature);
    }

    if (verbose) {
      System.err.println(term);
      for (String feature : featureValues.keySet()) {
        System.err.println("\t" + feature + "\t" + featureValues.get(feature) + "\t" + featureWeights.get(feature));
      }
      System.err.println("\n");
    }
    return weight;
  }

  private double computeWeight(String term1, String term2, NodeParameters np, Parameters queryParams) throws Exception {
    
    // override default weights
    Map<String, Double> featureWeights = new HashMap(defBigramWeights.size());
    for(String key : defBigramWeights.keySet()){
      featureWeights.put(key, np.get(key, queryParams.get(key, this.defBigramWeights.get(key))));
    }

    // start collecting feature values (should replace this spagetti code with feature-getters)
    Map<String, Double> featureValues = new HashMap(defUnigramWeights.size());
    featureValues.put("bi-const", 1.0);

    // prepare nodes (will be used several times)
    Node t1 = new Node("extents", term1);
    t1 = TextPartAssigner.assignPart(t1, queryParams, retrieval.getAvailableParts());
    Node t2 = new Node("extents", term2);
    t2 = TextPartAssigner.assignPart(t2, queryParams, retrieval.getAvailableParts());

    Node od = new Node("ordered");
    od.getNodeParameters().set("default", 1);
    od.addChild(t1);
    od.addChild(t2);
    od = retrieval.transformQuery(od, new Parameters());

    if ((featureWeights.get("bi-tf") != 0.0)
            || (featureWeights.get("bi-df") != 0.0)
            || featureWeights.get("bi-pmi") != 0.0) {
      NodeStatistics stats = retrieval.getNodeStatistics(od);

      if (stats.nodeFrequency > 0) {
        if (defProbFrequencies) {
          featureValues.put("bi-tf", (stats.nodeFrequency / (double) this.collStats.collectionLength));
          featureValues.put("bi-df", (stats.nodeDocumentCount / (double) this.collStats.documentCount));
        } else if (defLogFrequencies) {
          featureValues.put("bi-tf", Math.log(stats.nodeFrequency));
          featureValues.put("bi-df", Math.log(stats.nodeDocumentCount));
        } else {
          featureValues.put("bi-tf", (double) stats.nodeFrequency);
          featureValues.put("bi-df", (double) stats.nodeDocumentCount);
        }
      } else {
        featureValues.put("bi-tf", 0.0);
        featureValues.put("bi-df", 0.0);
      }

      if (stats.nodeFrequency > 0) {
        NodeStatistics t1stats = retrieval.getNodeStatistics(t1);
        NodeStatistics t2stats = retrieval.getNodeStatistics(t2);

        // SJH: should this be logged?... Math.log( p( 
        double pmi = ((double) stats.nodeFrequency) / ((double) t1stats.nodeFrequency * (double) t2stats.nodeFrequency);
        featureValues.put("bi-pmi", pmi);
      } else {
        featureValues.put("bi-pmi", 0.0);
      }
    }

    // google-bi-grams
    if (goog2Part != null
            && (featureWeights.get("bi-google-tf") != 0.0
            || featureWeights.get("bi-google-pmi") != 0.0)) {

      Node t = new Node("counts", term1 + "~" + term2);
      t.getNodeParameters().set("part", goog2Part);
      NodeStatistics stats = retrieval.getNodeStatistics(t);

      if (stats.nodeFrequency > 0) {
        if (defProbFrequencies) {
          featureValues.put("bi-google-tf", (stats.nodeFrequency / (double) this.google2PartStats.collectionLength));
        } else if (defLogFrequencies) {
          featureValues.put("bi-google-tf", Math.log(stats.nodeFrequency));
        } else {
          featureValues.put("bi-google-tf", (double) stats.nodeFrequency);
        }
      } else {
        featureValues.put("bi-google-tf", 0.0);
      }

      if (goog1Part != null && featureWeights.get("bi-google-pmi") != 0.0) {
        if (stats.nodeFrequency > 0.0) {
          Node t1g = new Node("counts", term1);
          t1g.getNodeParameters().set("part", goog1Part);
          Node t2g = new Node("counts", term2);
          t2g.getNodeParameters().set("part", goog1Part);
          NodeStatistics t1stats = retrieval.getNodeStatistics(t1g);
          NodeStatistics t2stats = retrieval.getNodeStatistics(t2g);

          double pmi = ((double) stats.nodeFrequency) / ((double) t1stats.nodeFrequency * (double) t2stats.nodeFrequency);
          featureValues.put("bi-google-pmi", pmi);
        } else {
          featureValues.put("bi-googl-pmi", 0.0);
        }
      }
    }

    // wiki-title-existences
    if (wikiTitlePart != null && featureWeights.get("bi-wt-e") != 0.0) {
      Node t = new Node("counts", term1 + "~" + term2);
      t.getNodeParameters().set("part", wikiTitlePart);
      NodeStatistics stats = retrieval.getNodeStatistics(t);

      if (stats.nodeFrequency > 0) {
        featureValues.put("bi-wt-e", 1.0);
      } else {
        featureValues.put("bi-wt-e", 0.0);
      }
    }

    // wiki-title-existences
    if (msnPart != null && featureWeights.get("bi-msn-e") != 0.0) {
      Node t = new Node("counts", term1 + "~" + term2);
      t.getNodeParameters().set("part", msnPart);
      NodeStatistics stats = retrieval.getNodeStatistics(t);

      // System.err.println("bi - msn-e : " + t.toString() + "\n\t" + stats.toString());
      if (stats.nodeFrequency > 0) {
        featureValues.put("bi-msn-e", 1.0);
      } else {
        featureValues.put("bi-msn-e", 0.0);
      }
    }

    // wiki group
    if (wikiGroup != null
            && ((featureWeights.get("bi-wt-tf") != 0.0)
            || (featureWeights.get("bi-wt-pmi") != 0.0))) {

      t1 = new Node("extents", term1);
      t1 = TextPartAssigner.assignPart(t1, queryParams, gRetrieval.getAvailableParts(wikiGroup));
      t2 = new Node("extents", term2);
      t2 = TextPartAssigner.assignPart(t2, queryParams, gRetrieval.getAvailableParts(wikiGroup));
      Node wt = new Node("ordered");
      wt.getNodeParameters().set("default", 1);
      wt.addChild(t1);
      wt.addChild(t2);

      NodeStatistics stats = gRetrieval.getNodeStatistics(wt, wikiGroup);

      if (stats.nodeFrequency > 0) {
        if (defProbFrequencies) {
          featureValues.put("bi-wt-tf", (stats.nodeFrequency / (double) this.wikiStats.collectionLength));
        } else if (defLogFrequencies) {
          featureValues.put("bi-wt-tf", Math.log(stats.nodeFrequency));
        } else {
          featureValues.put("bi-wt-tf", (double) stats.nodeFrequency);
        }
      } else {
        featureValues.put("bi-wt-tf", 0.0);
      }

      if (featureWeights.get("bi-wt-pmi") != 0.0) {
        if (stats.nodeFrequency > 0.0) {
          NodeStatistics t1stats = gRetrieval.getNodeStatistics(t1, wikiGroup);
          NodeStatistics t2stats = gRetrieval.getNodeStatistics(t2, wikiGroup);

          double pmi = ((double) stats.nodeFrequency) / ((double) t1stats.nodeFrequency * (double) t2stats.nodeFrequency);
          featureValues.put("bi-wt-pmi", pmi);
        } else {
          featureValues.put("bi-wt-pmi", 0.0);
        }
      }
    }

    // msn group
    if (msnGroup != null && featureWeights.get("bi-msn-tf") != 0.0) {
      t1 = new Node("extents", term1);
      t1 = TextPartAssigner.assignPart(t1, queryParams, gRetrieval.getAvailableParts(msnGroup));
      t2 = new Node("extents", term2);
      t2 = TextPartAssigner.assignPart(t2, queryParams, gRetrieval.getAvailableParts(msnGroup));
      Node mt = new Node("ordered");
      mt.getNodeParameters().set("default", 1);
      mt.addChild(t1);
      mt.addChild(t2);
      mt = gRetrieval.transformQuery(od, new Parameters(), msnGroup);

      NodeStatistics stats = gRetrieval.getNodeStatistics(mt, msnGroup);

      if (stats.nodeFrequency > 0) {
        if (defProbFrequencies) {
          featureValues.put("bi-msn-tf", (stats.nodeFrequency / (double) this.wikiStats.collectionLength));
        } else if (defLogFrequencies) {
          featureValues.put("bi-msn-tf", Math.log(stats.nodeFrequency));
        } else {
          featureValues.put("bi-msn-tf", (double) stats.nodeFrequency);
        }
      } else {
        featureValues.put("bi-msn-tf", 0.0);
      }

      if (featureWeights.get("bi-msn-pmi") != 0.0) {
        if (stats.nodeFrequency > 0) {
          NodeStatistics t1stats = gRetrieval.getNodeStatistics(t1, msnGroup);
          NodeStatistics t2stats = gRetrieval.getNodeStatistics(t2, msnGroup);

          double pmi = ((double) stats.nodeFrequency) / ((double) t1stats.nodeFrequency * (double) t2stats.nodeFrequency);
          featureValues.put("bi-msn-pmi", pmi);
        } else {
          featureValues.put("bi-msn-pmi", 0.0);
        }
      }
    }

    double weight = 0.0;

    for (String feature : featureValues.keySet()) {
      weight += featureValues.get(feature) * featureWeights.get(feature);
    }

    if (verbose) {
      System.err.println(term1 + " ~ " + term2);
      for (String feature : featureValues.keySet()) {
        System.err.println("\t" + feature + "\t" + featureValues.get(feature) + "\t" + featureWeights.get(feature));
      }
      System.err.println("\n");
    }
    return weight;
  }
}
