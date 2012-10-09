// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * A small class to echo the Document as is down the pipeline.
 * This is useful when the parser in fact does all the work already.
 * @author irmarc
 */

@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class StringPoolTokenizer extends StandardStep<Document, Document> {
  StringPooler pooler;
  public StringPoolTokenizer(TupleFlowParameters parameters) {
    pooler = new StringPooler();
  }

  @Override
  public void process(Document document) throws IOException {
    pooler.transform(document);
    processor.process(document);
  }
}