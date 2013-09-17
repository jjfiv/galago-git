// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * A small class to echo the Document as is down the pipeline.
 * This is useful when the parser in fact does all the work already.
 * @author irmarc
 */

@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class IdentityTokenizer extends Tokenizer {

  @Override
  public void tokenize(Document input) {
    assert(input.terms != null) : "IdentityTokenizer assumes terms was filled out by the parser...";
  }
}