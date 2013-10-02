// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tokenize;

import java.util.Arrays;
import org.lemurproject.galago.core.parse.Document;

/**
 *
 * @author jfoley
 */
public class NewlineTokenizer extends Tokenizer {

  @Override
  public void tokenize(Document input) {
    input.terms = Arrays.asList(input.text.split("\n"));
  }
}
