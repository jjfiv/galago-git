// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

/**
 * Currently represents the context that the entire query processor shares.
 * This is the most basic context we use.
 *
 * @author irmarc
 */
public class ScoringContext {

  public ScoringContext() {}

  public ScoringContext(int d, int l) {
    document = d; length = l;
  }

  public int document;
  public int length;
}
