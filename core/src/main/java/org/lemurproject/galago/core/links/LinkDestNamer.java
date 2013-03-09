// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.links;

import java.io.IOException;
import org.lemurproject.galago.core.types.DocumentUrl;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.types.ExtractedLink", order = {"+destUrl"})
@OutputClass(className = "org.lemurproject.galago.core.types.ExtractedLink", order = {"+destUrl"})
public class LinkDestNamer extends StandardStep<ExtractedLink, ExtractedLink> {

  TypeReader<ExtractedLink> extractedLinks;
  TypeReader<DocumentUrl> documentUrls;
  DocumentUrl current;
  Counter internalLinks;
  Counter externalLinks;
  boolean acceptExternalUrls;

  @SuppressWarnings("unchecked")
  public LinkDestNamer(TupleFlowParameters parameters) throws IOException {
    String documentUrlName = parameters.getJSON().getString("destNameStream");
    documentUrls = parameters.getTypeReader(documentUrlName);
    current = documentUrls.read();

    acceptExternalUrls = parameters.getJSON().getBoolean("acceptExternalUrls");

    internalLinks = parameters.getCounter("Internal Links");
    externalLinks = parameters.getCounter("External Links");
  }

  @Override
  public void process(ExtractedLink link) throws IOException {

    // while current.url preceeds destUrl -- read on
    while (current != null && Utility.compare(current.url, link.destUrl) < 0) {
      current = documentUrls.read();
    }

    if (current != null && Utility.compare(current.url, link.destUrl) == 0) {
      link.destName = current.identifier;
    }

    if (acceptExternalUrls && link.destName.isEmpty()) {
      link.destName = "EXT:" + link.destUrl;
      if(externalLinks != null) externalLinks.increment();
    } else {
      if(internalLinks != null) internalLinks.increment();
    }

    // only named destinations can be emited.
    if (!link.destName.isEmpty()) {
      processor.process(link);
    }
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!Verification.requireParameters(new String[]{"destNameStream"}, parameters.getJSON(), handler)) {
      return;
    }
  }
}
