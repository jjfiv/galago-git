// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.index.corpus;

import org.junit.Test;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class KeyValuePairToDocumentTest {
  private static final class DocumentProcessor implements Processor<Document> {
    Document document;
    public void process(Document document) {
      this.document = document;
    }
    public void close() throws IOException { }
  }

  @Test
  public void testProcess() throws Exception {
    KeyValuePairToDocument doc = new KeyValuePairToDocument();
    DocumentProcessor processor = new DocumentProcessor();

    ByteArrayOutputStream array = new ByteArrayOutputStream();
    ObjectOutputStream output = new ObjectOutputStream(array);
    Document document = new Document();
    document.identifier = 10;
    document.text = "This is text";
    document.metadata.put("hi", "there");
    document.name = "DOC1";
    output.writeObject(document);

    byte[] key = Utility.fromLong(document.identifier);
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
