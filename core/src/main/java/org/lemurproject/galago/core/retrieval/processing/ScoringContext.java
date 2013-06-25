// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

/**
 * Currently represents the context that the entire query processor shares. This
 * is the most basic context we use.
 *
 * @author irmarc, sjh
 */
public class ScoringContext {

  public long document;
  // indicates when nodes can/can't cache data
  // -- useful for passage or extent retrieval.
  public boolean cachable = true;

  public ScoringContext() {
  }

  public ScoringContext(long doc) {
    this.document = doc;
  }
}
