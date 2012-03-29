/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal.optimize;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Determines whether or not the current query tree can be run
 * with delta functions.

 * @author irmarc
 */
public class DeltaCheckTraversal extends Traversal {

  public DeltaCheckTraversal() {
  }

  @Override
  public void beforeNode(Node object) throws Exception {
  }

  @Override
  public Node afterNode(Node original) throws Exception {
    return original;
  }
}
