// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.types.SerializedParameters;

/**
 *
 * @author trevor
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.NumberedDocumentData")
@OutputClass(className = "org.lemurproject.galago.tupleflow.types.SerializedParameters", order = {"+parameters"})
public class CollectionLengthCounter extends StandardStep<NumberedDocumentData, SerializedParameters> {

  long collectionLength = 0;
  long documentCount = 0;

  public void process(NumberedDocumentData data) {
    collectionLength += data.textLength;
    documentCount += 1;
  }

  @Override
  public void close() throws IOException {
    Parameters p = new Parameters();
    p.set("statistics/collectionLength", collectionLength);
    p.set("statistics/documentCount", documentCount);
    processor.process(new SerializedParameters(p.toString()));
    processor.close();
  }
}
