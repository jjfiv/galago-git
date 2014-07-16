// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.core.types.NumberWordPosition;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.ByteUtil;

/**
 *
 * @author trevor, dmf
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.NumberWordPosition")
public class NumberedPostingsPositionExtractor extends StandardStep<Document, NumberWordPosition> {

  @Override
  public void process(Document object) throws IOException {
    for (int i = 0; i < object.terms.size(); i++) {
      String term = object.terms.get(i);
      if (term == null) {
        continue;
      }

      processor.process(new NumberWordPosition(object.identifier, ByteUtil.fromString(term), i));
    }
  }
}
