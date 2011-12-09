// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.KeyValueWriter;
import org.lemurproject.galago.core.types.DocumentFeature;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 * Writes the document indicator file 
 * 
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.DocumentFeature", order = {"+document"})
public class DocumentPriorWriter extends KeyValueWriter<DocumentFeature> {

  int lastDocument = -1;
  double maxObservedScore = Double.NEGATIVE_INFINITY;
  double minObservedScore = Double.POSITIVE_INFINITY;
  Counter written;

  /** Creates a new instance of DocumentLengthsWriter */
  public DocumentPriorWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    super(parameters, "Document indicators written");
    Parameters p = writer.getManifest();
    p.set("writerClass", DocumentPriorWriter.class.getName());
    p.set("readerClass", DocumentPriorReader.class.getName());

    written = parameters.getCounter("Priors Written");
  }

  @Override
  public GenericElement prepare(DocumentFeature docfeat) throws IOException {
    // word is ignored
    assert ((lastDocument < 0) || (lastDocument < docfeat.document)) : "DocumentPriorWriter keys must be unique and in sorted order.";

    maxObservedScore = Math.max(maxObservedScore, docfeat.value);
    minObservedScore = Math.min(minObservedScore, docfeat.value);
    GenericElement element = new GenericElement(Utility.fromInt((int)docfeat.document), Utility.fromDouble(docfeat.value));

    if (written != null) {
      written.increment();
    }
    return element;
  }

  @Override
  public void close() throws IOException {
    Parameters p = writer.getManifest();
    p.set("maxScore", this.maxObservedScore);
    p.set("minScore", this.minObservedScore);
    super.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("KeyValueWriters require a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, handler);
  }
}
