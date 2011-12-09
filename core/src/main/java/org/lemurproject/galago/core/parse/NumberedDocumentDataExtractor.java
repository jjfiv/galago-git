// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import java.util.HashSet;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * Similar to DocumentDataExtractor:
 * Copies a few pieces of metadata about a document (name, url, length) from
 * a document object. Additionally maintains a document identifier.
 * 
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.NumberedDocumentData")
@Verified
public class NumberedDocumentDataExtractor extends StandardStep<Document, NumberedDocumentData> {

  @Override
  public void process(Document document) throws IOException {
    NumberedDocumentData data = new NumberedDocumentData();
    data.identifier = document.name;
    data.url = "";
    if (document.metadata.containsKey("url")) {
      data.url = document.metadata.get("url");
    }
    data.textLength = document.terms.size();
    data.number = document.identifier;

    HashSet<String> tagSet = new HashSet();
    for (Tag tag : document.tags) {
      tagSet.add(tag.name);
    }

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String tag : tagSet) {
      if (!first) {
        sb.append(",");
      }
      sb.append(tag);
      first = false;
    }
    data.fieldList = sb.toString();

    processor.process(data);
  }
}
