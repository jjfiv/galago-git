// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.index.corpus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import junit.framework.TestCase;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class KeyValuePairToDocumentTest extends TestCase {
    
    public KeyValuePairToDocumentTest(String testName) {
        super(testName);
    }

    public class DocumentProcessor implements Processor<Document> {
        Document document;

        public void process(Document document) {
            this.document = document;
        }

        public void close() throws IOException {
        }
    }

    public void testProcess() throws Exception {
        KeyValuePairToDocument doc = new KeyValuePairToDocument();
        DocumentProcessor processor = new DocumentProcessor();
        
        ByteArrayOutputStream array = new ByteArrayOutputStream();
        ObjectOutputStream output = new ObjectOutputStream(array);
        Document document = new Document();
        document.text = "This is text";
        document.metadata.put("hi", "there");
        document.name = "DOC1";
        output.writeObject(document);

        byte[] key = Utility.fromString(document.name);
        byte[] value = array.toByteArray();
        KeyValuePair pair = new KeyValuePair(key, value);

        doc.setProcessor(processor);
        doc.process(pair);

        assertEquals(processor.document.name, document.name);
        assertEquals(processor.document.text, document.text);
        assertEquals(processor.document.metadata.size(), document.metadata.size());
        assertEquals(processor.document.metadata.get("hi"), "there");
    }
}
