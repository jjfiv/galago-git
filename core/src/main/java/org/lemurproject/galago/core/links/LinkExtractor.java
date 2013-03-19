/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links;

import java.io.IOException;
import java.net.URL;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.core.types.ExtractedLinkIndri;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.ExtractedLinkIndri")
public class LinkExtractor extends StandardStep<Document, ExtractedLinkIndri> {

  private boolean acceptLocalLinks;
  private boolean acceptNoFollowLinks;

  public LinkExtractor(TupleFlowParameters parameters) {
    acceptLocalLinks = parameters.getJSON().get("acceptLocalLinks", true);
    acceptNoFollowLinks = parameters.getJSON().get("acceptNoFollowLinks", true);
  }

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
  public void process(Document document) throws IOException {
    String sourceUrl = document.metadata.get("url");

    if (sourceUrl == null) {
      return;
    }
    URL base = new URL(sourceUrl);

    for (Tag t : document.tags) {
      if (t.name.equals("base")) {
        try {
          base = new URL(base, t.attributes.get("href"));
        } catch (Exception e) {
          // this can happen when the link protocol is unknown
          base = new URL(sourceUrl);
          continue;
        }
      }
    }

    for (Tag t : document.tags) {
      if (t.name.equals("a")) {
        String destSpec = t.attributes.get("href");
        URL destUrlObject = null;
        String destUrl = null;

        try {
          destUrlObject = new URL(base, destSpec);
          destUrl = destUrlObject.toString();
        } catch (Exception e) {
          // this can happen when the link protocol is unknown
          continue;
        }

        boolean linkIsLocal = destUrlObject.getHost().equals(base.getHost());

        // if we're filtering out local links, there's no need to continue
        if (linkIsLocal && acceptLocalLinks == false) {
          continue;
        }

        // need to output indri links initially - 
        ExtractedLinkIndri link = new ExtractedLinkIndri();
        link.filePath = document.filePath;
        link.fileLocation = document.fileLocation;

        link.srcName = document.name;
        link.srcUrl = scrubUrl(sourceUrl);
        link.destName = "";
        link.destUrl = scrubUrl(destUrl);
        // anchor text is extracted as a raw string, only white space is normalized
        if (t.charEnd > t.charBegin) {
          link.anchorText = document.text.substring(t.charBegin, t.charEnd);
        } else {
          link.anchorText = "";
        }
        link.anchorText = link.anchorText.replaceAll("\\s+", " ").trim();

        // REMOVE lone tags <img ...> from anchor text
        link.anchorText = link.anchorText.replaceAll("^<[^>]*>$", "");

        // System.out.println("Discovered link: " + link.toString());
        if (t.attributes.containsKey("rel") && t.attributes.get("rel").equals("nofollow")) {
          link.noFollow = true;
        } else {
          link.noFollow = false;
        }

        boolean acceptable = (acceptNoFollowLinks || link.noFollow == false);

        if (acceptable) {
          processor.process(link);
        }
      }
    }
  }
}
