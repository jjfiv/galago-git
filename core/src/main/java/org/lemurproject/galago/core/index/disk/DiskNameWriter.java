// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.merge.DocumentNameMerger;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;

/**
 * Writes a btree mapping from document names to document numbers
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.NumberedDocumentData", order = {"+number"})
public class DiskNameWriter implements Processor<NumberedDocumentData> {

  DiskBTreeWriter writer;
  NumberedDocumentData last = null;
  Counter documentNamesWritten = null;

  public DiskNameWriter(TupleFlowParameters parameters) throws IOException {
    documentNamesWritten = parameters.getCounter("Document Names Written");
    // make a folder
    String filename = parameters.getJSON().getString("filename");

    Parameters p = parameters.getJSON();
    p.set("writerClass", DiskNameWriter.class.getName());
    p.set("mergerClass", DocumentNameMerger.class.getName());
    p.set("readerClass", DiskNameReader.class.getName());

    writer = new DiskBTreeWriter(filename, p);
  }

  @Override
  public void process(NumberedDocumentData ndd) throws IOException {
    if (last == null) {
      last = ndd;
    }

    assert last.number <= ndd.number;
    assert ndd.identifier != null;

    writer.add(new GenericElement(
            Utility.fromLong(ndd.number),
            Utility.fromString(ndd.identifier)));

    if (documentNamesWritten != null) {
      documentNamesWritten.increment();
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("DocumentNameWriter requires a 'filename' parameter.");
      return;
    }
  }
}
