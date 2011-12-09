// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.structured;

/**
 * The context used for passage retrieval.
 *
 * @author irmarc
 */
public class PassageScoringContext extends ScoringContext {
  
  public int begin;
  public int end;
}
