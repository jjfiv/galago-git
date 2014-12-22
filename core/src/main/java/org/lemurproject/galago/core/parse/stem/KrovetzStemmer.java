// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse.stem;

import org.lemurproject.galago.krovetz.KStem;

public class KrovetzStemmer extends Stemmer {
  KStem kstem;

  public KrovetzStemmer() {
    kstem = new KStem();
  }

  @Override
  protected String stemTerm(String term) {
    return kstem.stemTerm(term);
  }
}
