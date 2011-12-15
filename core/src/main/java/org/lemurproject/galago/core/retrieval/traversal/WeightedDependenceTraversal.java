// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.retrieval.GroupRetrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.MalformedQueryException;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Weighted Sequential Dependency Model
 *   model is structurally similar to the Sequential Dependency Model,
 *   however node weights are the linear combination of some node features
 *
 * In particular the weight for a node "term" is determined to be:
 *  weight("term") =  unigram_constant
 *    + cf(term) * unigram_cf_lambda
 *    + df(term) * unigram_df_lambda
 *    + wiki_title_cf(term) * unigram_wiki_lambda
 *
 * bigram weights are determined using a similar method.
 *
 * The constant and lambda values are tunable parameters of the model.
 *
 * @author sjh
 */
public class WeightedDependenceTraversal implements Traversal {

  private GroupRetrieval retrieval;
  private Parameters unigramWeights;
  private Parameters bigramWeights;

  public WeightedDependenceTraversal(Retrieval retrieval) {
    if (retrieval instanceof GroupRetrieval) {
      this.retrieval = (GroupRetrieval) retrieval;
      Parameters parameters = retrieval.getGlobalParameters();

      unigramWeights = new Parameters();
      unigramWeights.set("const", parameters.get("uniconst", 0.8));
      unigramWeights.set("tf", parameters.get("unitf", -0.025));
      unigramWeights.set("df", parameters.get("unidf", -0.025));
      unigramWeights.set("wiki", parameters.get("uniwiki", 0.05));

      bigramWeights = new Parameters();
      bigramWeights.set("const", parameters.get("biconst", 0.1));
      bigramWeights.set("tf", parameters.get("bitf", 0.0));
      bigramWeights.set("df", parameters.get("bidf", 0.0));
      bigramWeights.set("wiki", parameters.get("biwiki", 0.0));

    } else {
      retrieval = null;

    }
  }

  @Override
  public void beforeNode(Node original) throws Exception {
  }

  @Override
  public Node afterNode(Node original) throws Exception {
    if (original.getOperator().equals("wsdm")) {

      assert this.retrieval != null : this.getClass().getName() + " requires a group retrieval to run.";
      assert this.retrieval.getGroups().contains("wiki") : this.getClass().getName() + " requires a 'wiki' index to run.";

      // First check format - should only contain text node children
      List<Node> children = original.getInternalNodes();
      for (Node child : children) {
        if (child.getOperator().equals("text") == false) {
          throw new MalformedQueryException("seqdep operator needs text-only children");
        }
      }

      // formatting is ok - now reassemble
      ArrayList<Node> newChildren = new ArrayList();
      NodeParameters newWeights = new NodeParameters();

      for (Node child : children) {
        double weight = computeWeight(child.getDefaultParameter());
        weight = weight / children.size();
        newWeights.set(Integer.toString(newChildren.size()), weight);
        newChildren.add(child.clone());
      }

      for (int i = 0; i < (children.size() - 1); i++) {
        ArrayList<Node> pair = new ArrayList();
        pair.add(children.get(i));
        pair.add(children.get(i + 1));
        double weight = computeWeight(pair.get(0).getDefaultParameter(), pair.get(1).getDefaultParameter());
        weight = weight / (children.size() - 1);

        newWeights.set(Integer.toString(newChildren.size()), weight);
        newChildren.add(new Node("od", new NodeParameters(1), Node.cloneNodeList(pair)));

        newWeights.set(Integer.toString(newChildren.size()), weight);
        newChildren.add(new Node("uw", new NodeParameters(8), Node.cloneNodeList(pair)));
      }

      Node wdm = new Node("combine", newWeights, newChildren, original.getPosition());
      return wdm;
    } else {
      return original;
    }
  }

  private double computeWeight(String text) throws Exception {
    double const_w = 0.0;
    double tf_w = 0.0;
    double df_w = 0.0;
    double wf_w = 0.0;

    const_w = unigramWeights.getDouble("const");

    if ((unigramWeights.getDouble("tf") != 0.0)
            && (unigramWeights.getDouble("df") != 0.0)) {
      NodeStatistics c_stats = this.retrieval.nodeStatistics(text);
      double mle_tf = (c_stats.nodeFrequency + 1) / c_stats.collectionLength;
      double mle_df = (c_stats.nodeDocumentCount + 1) / c_stats.documentCount;
      tf_w = unigramWeights.getDouble("tf") * Math.log(mle_tf);
      df_w = unigramWeights.getDouble("df") * Math.log(mle_df);
      
//      tf_w = unigramWeights.getDouble("tf") * Math.log(c_stats.nodeFrequency + 1);
//      df_w = unigramWeights.getDouble("df") * Math.log(c_stats.nodeDocumentCount + 1);
    }
    if (unigramWeights.getDouble("wiki") != 0.0) {
      NodeStatistics w_stats = this.retrieval.nodeStatistics(text, "wiki");
      double mle_wf = (w_stats.nodeFrequency + 1) / w_stats.collectionLength;
      wf_w = unigramWeights.getDouble("wiki") * Math.log(mle_wf);
//      wf_w = unigramWeights.getDouble("wiki") * Math.log(w_stats.nodeFrequency + 1);
    }
    return const_w + tf_w + df_w + wf_w;
  }

  private double computeWeight(String text1, String text2) throws Exception {
    double const_w = 0.0;
    double tf_w = 0.0;
    double df_w = 0.0;
    double wf_w = 0.0;

    const_w = bigramWeights.getDouble("const");

    if ((bigramWeights.getDouble("tf") != 0.0)
            && (bigramWeights.getDouble("df") != 0.0)) {
      NodeStatistics c_stats = this.retrieval.nodeStatistics("#uw:8(" + text1 + " " + text2 + ")");
      double mle_tf = c_stats.nodeFrequency / c_stats.collectionLength;
      double mle_df = c_stats.nodeDocumentCount / c_stats.documentCount;
      tf_w = unigramWeights.getDouble("tf") * Math.log(mle_tf);
      df_w = unigramWeights.getDouble("df") * Math.log(mle_df);

//      tf_w = bigramWeights.getDouble("tf") * Math.log(c_stats.nodeFrequency + 1);
//      df_w = bigramWeights.getDouble("df") * Math.log(c_stats.nodeDocumentCount + 1);
    }
    if (bigramWeights.getDouble("wiki") != 0.0) {
      NodeStatistics w_stats = this.retrieval.nodeStatistics("#od:1(" + text1 + " " + text2 + ")", "wiki");

//      wf_w = bigramWeights.getDouble("wiki") * Math.log(w_stats.nodeFrequency + 1);
    }
    return const_w + tf_w + df_w + wf_w;
  }
}
