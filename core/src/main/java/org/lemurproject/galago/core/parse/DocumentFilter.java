// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import java.io.IOException;
import java.util.HashSet;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;

/**
 *
 * @author trevor
 */

@InputClass(className="org.lemurproject.galago.core.parse.Document")
@OutputClass(className="org.lemurproject.galago.core.parse.Document")
public class DocumentFilter extends StandardStep<Document, Document> {
    HashSet<String> docnos = new HashSet();
    
    /** Creates a new create of DocumentFilter */
    public DocumentFilter(TupleFlowParameters parameters) {
        Parameters p = parameters.getJSON();
        docnos.addAll(p.getList("identifier"));
    }
    
    public void process(Document document) throws IOException {
        if (docnos.contains(document.name))
            processor.process(document);
    }
    
    public Class<Document> getOutputClass() {
        return Document.class;
    }
    
    public Class<Document> getInputClass() {
        return Document.class;
    }
}
