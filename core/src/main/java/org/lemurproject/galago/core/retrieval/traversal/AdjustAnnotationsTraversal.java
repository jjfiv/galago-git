/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.processing.FilteredStatisticsScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;

/**
 *
 * @author irmarc
 */
public class AdjustAnnotationsTraversal extends Traversal {

  Retrieval retrieval;
  FilteredStatisticsScoringContext context;
  
  public AdjustAnnotationsTraversal(Retrieval r, FilteredStatisticsScoringContext c) {
    this.retrieval = r;
    this.context = c;
  }
  
  @Override
  public Node afterNode(Node newNode) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void beforeNode(Node object) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
