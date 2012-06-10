// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import junit.framework.TestCase;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class DocumentToKeyValuePairTest extends TestCase {

  public DocumentToKeyValuePairTest(String testName) {
    super(testName);
  }

  public class KeyValuePairProcessor implements KeyValuePair.Processor {

    KeyValuePair pair;

    @Override
    public void process(KeyValuePair pair) {
      this.pair = pair;
    }

    @Override
    public void close() throws IOException {
    }
  }

  public void testProcess() throws Exception {
    DocumentToKeyValuePair dkvp = new DocumentToKeyValuePair();
    KeyValuePairProcessor kvpProcessor = new KeyValuePairProcessor();
    dkvp.setProcessor(kvpProcessor);

    Document document = new Document();
    document.identifier = 1;
    document.text = "This is text.";
    document.name = "DOC2";
    document.metadata.put("this", "that");
    dkvp.process(document);

    KeyValuePair pair = kvpProcessor.pair;
    assertEquals(Utility.toInt(pair.key), 1);

    ByteArrayInputStream stream = new ByteArrayInputStream(pair.value);
    ObjectInputStream input = new ObjectInputStream(stream);
    Document result = (Document) input.readObject();

    assertEquals(result.identifier, document.identifier);
    assertEquals(result.text, document.text);
    assertEquals(result.name, document.name);
    assertEquals(result.metadata.size(), document.metadata.size());
    assertEquals(result.metadata.get("this"), "that");
  }
}
