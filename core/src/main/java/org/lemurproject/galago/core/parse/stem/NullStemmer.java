/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse.stem;

/**
 *
 * @author sjh
 */
public class NullStemmer extends Stemmer {

  @Override
  protected String stemTerm(String term) {
    return term;
  }
}
