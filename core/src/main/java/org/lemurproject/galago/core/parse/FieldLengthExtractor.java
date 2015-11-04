/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.ByteUtil;

import java.io.IOException;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.FieldLengthData")
public class FieldLengthExtractor extends StandardStep<Document, FieldLengthData> {


  @Override
  public void process(Document doc) throws IOException {
    processor.process(new FieldLengthData(ByteUtil.fromString("document"), doc.identifier, doc.terms.size()));

    TObjectIntHashMap<String> fieldLengths = new TObjectIntHashMap<>();
    for (Tag tag : doc.tags) {
      int len = tag.end - tag.begin;
      fieldLengths.adjustOrPutValue(tag.name, len, len);
    }

    for (String field : fieldLengths.keySet()) {
      processor.process(new FieldLengthData(ByteUtil.fromString(field), doc.identifier, fieldLengths.get(field)));
    }
  }
}
