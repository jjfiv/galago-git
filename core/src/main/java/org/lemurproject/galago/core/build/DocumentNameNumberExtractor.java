// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.build;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.DocumentNameId;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.ByteUtil;

import java.io.IOException;

/**
 * Similar to DocumentDataExtractor:
 * Copies a few pieces of metadata about a document (name, url, length) from
 * a document object. Additionally maintains a document identifier.
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.DocumentNameId")
@Verified
public class DocumentNameNumberExtractor extends StandardStep<Document, DocumentNameId> {

  @Override
  public void process(Document document) throws IOException {
    DocumentNameId data = new DocumentNameId();
    data.name = ByteUtil.fromString(document.name);
    data.id = document.identifier;
    processor.process(data);
  }
}

