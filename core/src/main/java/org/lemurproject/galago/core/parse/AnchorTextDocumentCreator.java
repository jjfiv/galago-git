// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.core.types.IdentifiedLink;

/**
 * From an IdentifiedLink object, this class constructs a document containing
 * only anchor text.
 * 
 * @author trevor
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.IdentifiedLink")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class AnchorTextDocumentCreator extends StandardStep<IdentifiedLink, Document> {
    TagTokenizer tokenizer = new TagTokenizer();
    ArrayList<IdentifiedLink> links = new ArrayList<IdentifiedLink>();
    String lastDocument = null;

    /**
     * This method takes the text from a link object, tokenizes it,
     * then adds it to a document object.
     */
    public void process(IdentifiedLink link) throws IOException {
        if (lastDocument != null && !lastDocument.equals(link.identifier)) {
            flush();
        }
        links.add(link);
        lastDocument = link.identifier;
    }

    public void flush() throws IOException {
        Document document = new Document();

        if (links.size() == 0) {
            return;
        } else if (links.size() == 1) {
            IdentifiedLink link = links.get(0);

            document.text = link.anchorText;
            document.name = link.identifier;
        } else {
            StringBuilder builder = new StringBuilder();

            for (IdentifiedLink link : links) {
                builder.append(link.anchorText);
                builder.append(' ');
            }

            document.text = builder.toString();
            document.name = links.get(0).identifier;
        }

        document.terms = null;
        document.metadata = new HashMap<String, String>();
        document.tags = new ArrayList<Tag>();

        // parse the text into pieces
        tokenizer.process(document);

        // send it on to the next stage
        processor.process(document);
        lastDocument = null;
        links.clear();
    }

    @Override
    public void close() throws IOException {
        flush();
        super.close();
    }

    public Class<IdentifiedLink> getInputClass() {
        return IdentifiedLink.class;
    }

    public Class<Document> getOutputClass() {
        return Document.class;
    }
}
