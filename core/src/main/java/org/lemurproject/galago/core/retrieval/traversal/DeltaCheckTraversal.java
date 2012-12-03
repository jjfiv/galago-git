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
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * This ensures that the current query tree can be run using the delta scoring
 * model. If not, it shuts it off in the parameters.
 *
 *
 * @author irmarc
 */
public class DeltaCheckTraversal extends Traversal {

  Parameters qp;
  HashSet<Node> deltas;
  Retrieval retrieval;
  int level;

  public DeltaCheckTraversal(Retrieval r, Parameters queryParams) {
    qp = queryParams;
    retrieval = r;
    deltas = new HashSet<Node>();
    level = 0;
  }

  @Override
  public Node afterNode(Node original) throws Exception {
    level--;
    if (isDeltaCapable(original)) {
      deltas.add(original);
    }

    // Final judgment
    if (level == 0) {
      // sjh: turned off for now - producing some errors in tests.
      // qp.set("deltaReady", deltas.contains(original));
    }

    return original;
  }

  @Override
  public void beforeNode(Node object) throws Exception {
    level++;
//    if (object.getOperator().equals("feature")) {
//      int c = (int) qp.get("numScorers", 0L);
//      qp.set("numScorers", c + 1);
//    }
  }

  private boolean isDeltaCapable(Node n) throws Exception {
    if (retrieval == null || n == null) {
      return false;
    }
    NodeType nt = retrieval.getNodeType(n);
    if (DeltaScoringIterator.class.isAssignableFrom(nt.getIteratorClass())) {
      return true;
    }

    // No children == no good
    if (n.numChildren() == 0) {
      return false;
    }

    // Need to check the children,
    // also propagate weights downward in the hopes they can be used
    boolean isScoreCombiner = DisjunctionIterator.class.isAssignableFrom(nt.getIteratorClass()) &&
	ScoreIterator.class.isAssignableFrom(nt.getIteratorClass());
    boolean isOk = true;
    NodeParameters np = n.getNodeParameters();
    double total = 0;
    if (isScoreCombiner && np.get("norm", true)) {
      for (int i = 0; i < n.numChildren(); i++) {
        String key = Integer.toString(i);
        total += np.containsKey(key) ? np.getDouble(key) : 1.0;
      }
    }

    for (int i = 0; i < n.numChildren(); i++) {
      Node child = n.getChild(i);
      isOk &= deltas.contains(child);
      if (isScoreCombiner && qp.get("deltaWeightsSet", false) == false) {
        String key = Integer.toString(i);
        if (np.get("norm", true)) {
          if (np.containsKey(key)) {
            child.getNodeParameters().set("w",
                    n.getNodeParameters().getDouble(key) / total);
          } else {
            child.getNodeParameters().set("w", 1.0 / total);
          }
        } else {
          if (np.containsKey(key)) {
            child.getNodeParameters().set("w",
                    n.getNodeParameters().getDouble(key));
          } else {
            child.getNodeParameters().set("w", 1.0);
          }
        }
      }
    }
    return isOk;
  }
}
