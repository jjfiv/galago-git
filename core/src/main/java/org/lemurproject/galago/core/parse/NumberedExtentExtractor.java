// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import java.io.IOException;
import org.lemurproject.galago.core.types.NumberedExtent;
import org.lemurproject.galago.utility.ByteUtil;

/**
 * Converts all tags from a document object into NumberedExtent tuples.
 * @author trevor
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.NumberedExtent")
public class NumberedExtentExtractor extends StandardStep<Document, NumberedExtent> {

  public void process(Document document) throws IOException {
    for (Tag tag : document.tags) {
      processor.process(new NumberedExtent(ByteUtil.fromString(tag.name),
              document.identifier,
              tag.begin,
              tag.end));
    }
  }
}
