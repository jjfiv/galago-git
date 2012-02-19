// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

public abstract class ContextualAbstractIndicator extends AbstractIndicator implements ContextualIterator {

  ScoringContext context;

  public ContextualAbstractIndicator(NodeParameters p, ValueIterator[] childIterators) {
    super(p, childIterators);
  }

  public ScoringContext getContext() {
    return context;
  }

  public void setContext(ScoringContext c) {
    this.context = c;
  }
}