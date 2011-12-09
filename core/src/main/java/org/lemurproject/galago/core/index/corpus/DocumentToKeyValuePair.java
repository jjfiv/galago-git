// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPOutputStream;
import org.lemurproject.galago.core.parse.Document;

import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * Writes documents to a file
 *  - new output file is created in the folder specified by "filename"
 *  - document.name -> output-file, byte-offset is passed on
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class DocumentToKeyValuePair extends StandardStep<Document, KeyValuePair> implements KeyValuePair.Source {

    boolean compressed;

    public DocumentToKeyValuePair() {
        compressed = false; // used for testing
    }

    public DocumentToKeyValuePair(TupleFlowParameters parameters) {
        compressed = parameters.getJSON().get("compressed", true);
    }

    public void process(Document document) throws IOException {
        ByteArrayOutputStream array = new ByteArrayOutputStream();
        ObjectOutputStream output;
        if (compressed) {
            output = new ObjectOutputStream(new GZIPOutputStream(array));
        } else {
            output = new ObjectOutputStream(array);
        }

        output.writeObject(document);
        output.close();

        byte[] key = Utility.fromString(document.name);
        byte[] value = array.toByteArray();
        KeyValuePair pair = new KeyValuePair(key, value);
        processor.process(pair);

    }
}
