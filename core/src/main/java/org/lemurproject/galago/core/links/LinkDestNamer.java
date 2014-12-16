// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.links;

import java.io.IOException;
import org.lemurproject.galago.core.types.DocumentUrl;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.core.types.ExtractedLinkIndri;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.utility.CmpUtil;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.types.ExtractedLinkIndri", order = {"+destUrl"})
@OutputClass(className = "org.lemurproject.galago.core.types.ExtractedLinkIndri", order = {"+destUrl"})
public class LinkDestNamer extends StandardStep<ExtractedLinkIndri, ExtractedLinkIndri> {

  public static final String EXTERNAL_PREFIX = "EXT:";
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
  public void process(ExtractedLinkIndri link) throws IOException {

    // while current.url preceeds destUrl -- read on
    while (current != null && CmpUtil.compare(current.url, link.destUrl) < 0) {
      current = documentUrls.read();
    }

    if (current != null && current.url.equals(link.destUrl)) {
      link.destName = current.identifier;
      link.filePath = current.filePath;
      link.fileLocation = current.fileLocation;
    }

    if (acceptExternalUrls && link.destName.isEmpty()) {
      link.destName = EXTERNAL_PREFIX + link.destUrl;
      if (externalLinks != null) {
        externalLinks.increment();
      }
    } else {
      if (internalLinks != null) {
        internalLinks.increment();
      }
    }

    // only named destinations can be emited.
    if (!link.destName.isEmpty()) {
      processor.process(link);
    }
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!Verification.requireParameters(new String[]{"destNameStream"}, parameters.getJSON(), store)) {
      return;
    }
  }
}
