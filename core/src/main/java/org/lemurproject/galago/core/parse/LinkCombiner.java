// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.tupleflow.ExNihiloSource;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;
import java.io.IOException;
import org.lemurproject.galago.core.types.DocumentData;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.core.types.IdentifiedLink;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.Counter;

/**
 *
 * @author trevor
 */
@OutputClass(className = "org.lemurproject.galago.core.parse.DocumentLinkData")
public class LinkCombiner implements ExNihiloSource<IdentifiedLink>, IdentifiedLink.Source {

  TypeReader<ExtractedLink> extractedLinks;
  TypeReader<NumberedDocumentData> documentDatas;
  DocumentLinkData linkData;
  public Processor<DocumentLinkData> processor;
  Counter linksProcessed;

  @SuppressWarnings("unchecked")
  public LinkCombiner(TupleFlowParameters parameters) throws IOException {
    String extractedLinksName = parameters.getJSON().getString("extractedLinks");
    String documentDatasName = parameters.getJSON().getString("documentDatas");
    linksProcessed = parameters.getCounter("Links Combined");
    extractedLinks = parameters.getTypeReader(extractedLinksName);
    documentDatas = parameters.getTypeReader(documentDatasName);
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }

  void match(NumberedDocumentData docData, ExtractedLink link) {
    if (linkData == null) {
      linkData = new DocumentLinkData();
      linkData.identifier = docData.number;
      linkData.url = docData.url;
      linkData.textLength = docData.textLength;
    }

    linkData.links.add(link);
  }

  void flush() throws IOException {
    if (linkData != null) {
      processor.process(linkData);
      if (linksProcessed != null) {
        linksProcessed.incrementBy(linkData.links.size());
      }
    }
  }

  public void run() throws IOException {
    ExtractedLink link = extractedLinks.read();
    NumberedDocumentData docData = documentDatas.read();
    while (docData != null && link != null) {
      int result = link.destUrl.toLowerCase().compareTo(docData.url.toLowerCase());
      System.out.println("Comparing destination url: " + link.destUrl + " with doc: "+ docData.url);
      if (result == 0) {
        match(docData, link);
        link = extractedLinks.read();
      } else {
        if (result < 0) {
          link = extractedLinks.read();
        } else {
          flush();
          docData = documentDatas.read();
        }
      }
    }
    flush();
    processor.close();
  }

  public Class<IdentifiedLink> getOutputClass() {
    return IdentifiedLink.class;
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!Verification.requireParameters(new String[]{"extractedLinks", "documentDatas"},
            parameters.getJSON(), handler)) {
      return;
    }

    String extractedLinksName = parameters.getJSON().getString("extractedLinks");
    String documentDatasName = parameters.getJSON().getString("documentDatas");

    //Verification.verifyTypeReader(extractedLinksName, ExtractedLink.class, parameters, handler);
    //Verification.verifyTypeReader(documentDatasName, DocumentData.class, parameters, handler);
  }
}
