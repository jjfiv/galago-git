// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.processing;

/**
 * The context used for passage retrieval.
 *
 * @author irmarc
 */
public class PassageScoringContext extends ScoringContext {
  public PassageScoringContext() {
    super();
    begin = 0;
    end = Integer.MAX_VALUE;
  }
  public int begin;
  public int end;
}
