// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

/**
 * Context objects allow information to be passed in-to, out-of, 
 * and across the iterator structure.
 * 
 * The setContext function call setContext recursively.
 *  - this allows a single setContext 
 *    to be called at the root of the tree.
 * 
 * @author irmarc
 */
public interface ContextualIterator {
  public void setContext(ScoringContext context);
  public ScoringContext getContext();
}
