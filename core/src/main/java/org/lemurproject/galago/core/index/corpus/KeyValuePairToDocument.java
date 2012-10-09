// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.xerial.snappy.SnappyInputStream;

/**
 * <p>This is used in conjunction with DocumentToKeyValuePair.  Since Document
 * is not a real Galago type, it needs to be converted to a KeyValuePair in order
 * to be passed between stages (or to a Sorter).</p>
 * 
 * @author trevor
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class KeyValuePairToDocument extends StandardStep<KeyValuePair, Document> {

  boolean compressed;

  public KeyValuePairToDocument() {
    compressed = false; // used for testing
  }

  public KeyValuePairToDocument(TupleFlowParameters parameters) {
    compressed = parameters.getJSON().get("compressed", true);
  }

  public void process(KeyValuePair object) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(object.value);
    Document document;

    try {
      if (compressed) {
        ObjectInputStream input = new ObjectInputStream(new SnappyInputStream(stream));
        document = (Document) input.readObject();
      } else {
        ObjectInputStream input = new ObjectInputStream(stream);
        document = (Document) input.readObject();
      }
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException("Unable to extract document from KeyValuePair." + ex.toString());
    }

    processor.process(document);
  }
}
