/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.traversal.optimize;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.processing.SoftDeltaScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.traversal.AnnotateCollectionStatistics;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
public class ReplaceEstimatedIteratorTraversal extends Traversal {

  boolean passedWindow;
  public SoftDeltaScoringContext context = null;
  LocalRetrieval lr;
  AnnotateCollectionStatistics annotator;
  Parameters queryParams;
  
  public ReplaceEstimatedIteratorTraversal(Retrieval r, Parameters queryParameters) {
    passedWindow = false;
    if (r != null && LocalRetrieval.class.isAssignableFrom(r.getClass())) {
      lr = (LocalRetrieval) r;
      try {
        annotator = new AnnotateCollectionStatistics(lr);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    queryParams = queryParameters;
  }

  @Override
  public Node afterNode(Node newNode) throws Exception {
    String op = newNode.getOperator();
    if (op.equals("ordered") || op.equals("unordered")) {
      passedWindow = false;
    } else if (op.equals("feature")) {
      Node child = newNode.getChild(0);
      if ((child.getOperator().equals("ordered") || child.getOperator().equals("unordered"))
              && lr != null && context != null) {
        // Let's make us an iterator - it sets itself to the context so no need to pass it up
        Node eligible = annotator.afterNode(newNode);
        MovableIterator iterator = lr.createIterator(queryParams, eligible, context);
      }
    }
    return newNode;
  }

  @Override
  public void beforeNode(Node object) throws Exception {
    if (object.getOperator().equals("mincount")) {
      int def = (int) object.getNodeParameters().getLong("default");
      if (def == 1) {
        object.setOperator("ordered");
      } else if (def == 8) {
        object.setOperator("unordered");
      } else {
        throw new IllegalArgumentException("Using an unknown windows size!");
      }
      passedWindow = true;
    } else if (object.getOperator().equals("counts") && passedWindow) {
      object.setOperator("extents");
      NodeParameters np = object.getNodeParameters();
      String newPart = np.getString("part").replace("counts", "postings");
      np.set("part", newPart);
    } else if (object.getOperator().equals("feature")) {
      String feature = object.getDefaultParameter();
      feature = feature.replace("-est", "");
      object.getNodeParameters().set("default", feature);
    }
  }
}
