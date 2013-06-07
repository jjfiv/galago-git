/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.HashSet;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.DisjunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.FilteredIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * This ensures that the current query tree can be run using the delta scoring
 * model. If not, it shuts it off in the parameters.
 *
 * @author irmarc, sjh
 */
public class DeltaCheckTraversal extends Traversal {

  Retrieval retrieval;

  public DeltaCheckTraversal(Retrieval r) {
    retrieval = r;
  }

  @Override
  public Node afterTreeRoot(Node original, Parameters qp) throws Exception {

    if (qp.containsKey("deltaReady") && qp.getBoolean("deltaReady") == false) {
      // This means something already turned off the delta
      // scoring possibility - e.g. a filtering node.
      return original;
    }

    // if the node is deltaCapable
    if (isDeltaCapable(original)) {
      // ensure the correct processing model can be used
      qp.set("deltaReady", true);

      // now add w parameters to each node
      propagateWeights(original, qp);
    }

    return original;
  }

  private boolean isDeltaCapable(Node n) throws Exception {
    if (retrieval == null || n == null) {
      return false;
    }

    NodeType nt = retrieval.getNodeType(n);

    if (nt == null) {
      return false;
    }

    Class iteratorClass = nt.getIteratorClass();

    // Filters, currently, will not work (candidate checking skips them)
    if (FilteredIterator.class.isAssignableFrom(iteratorClass)) {
      return false;
    }

    // DeltaScoringIterators are required somewhere in the tree
    if (DeltaScoringIterator.class.isAssignableFrom(iteratorClass)) {
      return true;
    }

    // otherwise recursively check children
    // No children == no good
    if (n.numChildren() == 0) {
      return false;
    }

    // Need to check the children
    boolean isOk = true;

    for (int i = 0; i < n.numChildren(); i++) {
      Node child = n.getChild(i);
      isOk &= isDeltaCapable(child);
    }
    return isOk;
  }

  public void propagateWeights(Node n, Parameters qp) throws Exception {

    if (retrieval == null || n == null) {
      return;
    }
    NodeType nt = retrieval.getNodeType(n);
    Class iteratorClass = nt.getIteratorClass();

    // annotate weights on to children (if combine node, otherwise assumes equal weighting)
    boolean isScoreCombiner = DisjunctionIterator.class.isAssignableFrom(iteratorClass) && ScoreIterator.class.isAssignableFrom(iteratorClass);

    if (isScoreCombiner) {
      NodeParameters np = n.getNodeParameters();
      double weightSum = 1.0;

      if (isScoreCombiner && np.get("norm", true)) {
        weightSum = 0.0;
        for (int i = 0; i < n.numChildren(); i++) {
          String key = Integer.toString(i);
          weightSum += np.containsKey(key) ? np.getDouble(key) : 1.0;
        }
      }

      if (!n.getNodeParameters().containsKey("w")) {
        n.getNodeParameters().set("w", 1.0);
      }

      double thisNodeWeight = n.getNodeParameters().getDouble("w");

      // Can't normalize a negative weightSum, it would invert the relative ranking -- if so, don't normalize
      if (weightSum < 0.0) {
        weightSum = 1.0;
      }

      for (int i = 0; i < n.numChildren(); i++) {
        Node child = n.getChild(i);

        if (!child.getNodeParameters().containsKey("w") && qp.get("deltaWeightsSet", false) == false) {
          String cid = Integer.toString(i);
          child.getNodeParameters().set("w", thisNodeWeight * (n.getNodeParameters().get(cid, 1.0) / weightSum));
        }
        // recursively assign weights
        propagateWeights(child, qp);
      }
    }
  }

  // this node requires state -- does nothing for regular traversal functions
  @Override
  public void beforeNode(Node original, Parameters queryParameters) throws Exception {
    // nothing
  }

  @Override
  public Node afterNode(Node original, Parameters queryParameters) throws Exception {
    return original;
  }
}
