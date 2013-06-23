// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.merge.DocumentNameMerger;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;

/**
 *
 * Writes a mapping from document names to document numbers
 *
 * Does not assume that the data is sorted - as data would need to be sorted
 * into both key and value order - instead this class takes care of the
 * re-sorting - this may be inefficient, but docnames is a relatively small pair
 * of files
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

  public void process(int number, String identifier) throws IOException {
    byte[] docNum = Utility.fromInt(number);
    byte[] docName = Utility.fromString(identifier);

    writer.add(new GenericElement(docNum, docName));

    if (documentNamesWritten != null) {
      documentNamesWritten.increment();
    }
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
