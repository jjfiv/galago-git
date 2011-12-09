// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse.stem;

import org.tartarus.snowball.ext.englishStemmer;

/**
 *
 * @author trevor
 * sjh: modified to accept numbered documents as required.
 */
public class Porter2Stemmer extends Stemmer {

  englishStemmer stemmer = new englishStemmer();

  protected String stemTerm(String term) {
    String stem = term;
    stemmer.setCurrent(term);
    if (stemmer.stem()) {
      stem = stemmer.getCurrent();
    }
    return stem;
  }

}
