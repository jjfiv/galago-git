/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.IOException;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.FieldLengthData")
public class FieldLengthExtractor extends StandardStep<Document, FieldLengthData> {

  TObjectIntHashMap<String> fieldLengths = new TObjectIntHashMap();

  @Override
  public void process(Document doc) throws IOException {
    processor.process(new FieldLengthData(Utility.fromString("document"), doc.identifier, doc.terms.size()));

    fieldLengths.clear();
    for (Tag tag : doc.tags) {
      int len = tag.end - tag.end;
      fieldLengths.adjustOrPutValue(tag.name, len, len);
    }

    for (String field : fieldLengths.keySet()) {
      processor.process(new FieldLengthData(Utility.fromString(field), doc.identifier, fieldLengths.get(field)));
    }
  }
}
