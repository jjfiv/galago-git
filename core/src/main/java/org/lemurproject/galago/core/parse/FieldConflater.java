// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * For G3.1, we moved to JSON-modeled parameters. The new way to specify conflated fields is:
 *
 * "field" : [ "name1", "name2", { "source" : "name3", "destination" : "name1" } ]
 *
 * So, simple fields are "name1" and "name2", and the compound field, "name3", will be converted into
 * entries of "name1".
 *
 * Multiple conflations to one destination can be done by using a list in the source field:
 * { "source" : [ "name2", "name3", "name4" ], "name1" }
 *
 * @author trevor, irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
@Verified
public class FieldConflater extends StandardStep<Document, Document> {

  Parameters conflations;

  public FieldConflater(TupleFlowParameters parameters) {
    if (parameters.getJSON().isMap("tokenizer/conflations")) {
      conflations = parameters.getJSON().getMap("tokenizer/conflations");
    } else {
      conflations = new Parameters();
    }
  }

  public void process(Document document) throws IOException {
    for (Tag tag : document.tags) {
      if (conflations.isString(tag.name)) {
        tag.name = conflations.getString(tag.name);
      }
    }

    processor.process(document);
  }
}
