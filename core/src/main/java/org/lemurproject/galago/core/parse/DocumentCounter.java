// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.types.SerializedParameters;

/**
 *
 * output:
 * { "documentCount" :
 *    {"global" : xx
 *     "fieldName1" : yy
 *     "fieldName2" : zz
 *    }
 * }
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.NumberedDocumentData")
@OutputClass(className = "org.lemurproject.galago.tupleflow.types.SerializedParameters", order = {"+parameters"})
public class DocumentCounter extends StandardStep<NumberedDocumentData, SerializedParameters> {

  Parameters p;

  public DocumentCounter() {
    p = new Parameters();
    p.set("global", 0L);
  }

  public void process(NumberedDocumentData data) {
    p.set("global", p.get("global", 0L) + 1);
    if (data.fieldList.length() > 0) {
      for (String field : data.fieldList.split(",")) {
        p.set(field, p.get(field, 0L) + 1);
      }
    }
  }

  @Override
  public void close() throws IOException {
    Parameters output = new Parameters();
    output.set("documentCount", p);
    processor.process(new SerializedParameters(output.toString()));
    super.close();
  }
}
