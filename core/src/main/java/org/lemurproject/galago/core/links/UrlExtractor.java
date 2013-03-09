/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links;

import java.io.IOException;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.DocumentUrl;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.DocumentUrl")
public class UrlExtractor extends StandardStep<Document, DocumentUrl> {

  @Override
  public void process(Document doc) throws IOException {
    processor.process(new DocumentUrl(doc.name, doc.metadata.get("url")));
  }
}
