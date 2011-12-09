// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.core.types.AdditionalDocumentText;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.DocumentLinkData")
@OutputClass(className = "org.lemurproject.galago.core.types.AdditionalDocumentText")
public class AnchorTextCreator extends StandardStep<DocumentLinkData, AdditionalDocumentText> {

    Counter counter;

    public AnchorTextCreator(TupleFlowParameters parameters) {
      counter = parameters.getCounter("Anchors Created");
    }

    @Override
    public void process(DocumentLinkData object) throws IOException {
        AdditionalDocumentText additional = new AdditionalDocumentText();
        StringBuilder extraText = new StringBuilder();

        additional.identifier = object.identifier;
        for (ExtractedLink link : object.links) {
            extraText.append("<anchor>");
            extraText.append(link.anchorText);
            extraText.append("</anchor>");
        }
        additional.text = extraText.toString();

        processor.process(additional);
        if (counter != null) counter.increment();
    }
}
