// BSD License (http://lemurproject.org/galago-license)
package ciir.proteus.galago.parse;

import java.io.IOException;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * @author irmarc
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class PictureDocumentSerializer extends StandardStep<Document, KeyValuePair> {

  @Override
  public void process(Document document) throws IOException {
    String key = String.format("%s_%s",
            document.name,
            document.metadata.get("ordinal"));
    processor.process(new KeyValuePair(Utility.fromString(key),
            Document.serialize(document)));
  }
}
