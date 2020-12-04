// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse.stem;

import org.tartarus.snowball.ext.arabicStemmer;

public class SnowballArabicStemmer extends Stemmer {

  arabicStemmer stemmer = new arabicStemmer();

  @Override
  protected String stemTerm(String term) {
    String stem = term;
    stemmer.setCurrent(term);
    if (stemmer.stem()) {
      stem = stemmer.getCurrent();
    }
    return stem;
  }
}
