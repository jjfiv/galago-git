// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.HashMap;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.query.Node;

/**
 * Currently represents the context that the entire query processor shares. This
 * is the most basic context we use.
 *
 * The lengths are generally managed from this construct.
 *
 * @author irmarc
 */
public class ScoringContext {

  public int document;
  // indicates when nodes can/can't cache data
  // -- useful for passage or extent retrieval.
  public boolean cachable = true;

  // Diagnostic
  public HashMap<BaseIterator, Node> toNodes = new HashMap<BaseIterator, Node>();
}
