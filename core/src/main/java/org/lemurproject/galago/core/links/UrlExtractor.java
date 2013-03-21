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

  public String scrubUrl(String url) {
    // remove a leading pound sign
    if (url.charAt(url.length() - 1) == '#') {
      url = url.substring(0, url.length() - 1);        // make it lowercase
    }
    url = url.toLowerCase();

    // remove a port number, if it's the default number
    url = url.replace(":80/", "/");
    if (url.endsWith(":80")) {
      url = url.replace(":80", "");
    }
    // remove trailing slashes
    while (url.charAt(url.length() - 1) == '/') {
      url = url.substring(0, url.length() - 1);
    }
    return url.toLowerCase();
  }
  @Override
  public void process(Document doc) throws IOException {
    processor.process(new DocumentUrl(doc.name, scrubUrl(doc.metadata.get("url"))));
  }
}
