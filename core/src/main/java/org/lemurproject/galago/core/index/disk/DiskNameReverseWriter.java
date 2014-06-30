// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.merge.DocumentNameReverseMerger;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;

/**
 * 
 * Writes a mapping from document names to document numbers
 * 
 * Does not assume that the data is sorted
 *  - as data would need to be sorted into both key and value order
 *  - instead this class takes care of the re-sorting
 *  - this may be inefficient, but docnames is a relatively small pair of files
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.NumberedDocumentData", order = {"+identifier"})
public class DiskNameReverseWriter implements Processor<NumberedDocumentData> {

  DiskBTreeWriter writer;
  NumberedDocumentData last = null;
  Counter documentNamesWritten = null;

  public DiskNameReverseWriter(TupleFlowParameters parameters) throws IOException {
    documentNamesWritten = parameters.getCounter("Document Names Written");
    // make a folder
    String filename = parameters.getJSON().getString("filename");

    Parameters p = parameters.getJSON();
    p.set("writerClass", DiskNameReverseWriter.class.getName());
    p.set("mergerClass", DocumentNameReverseMerger.class.getName());
    p.set("readerClass", DiskNameReverseReader.class.getName());

    writer = new DiskBTreeWriter(filename, p);
  }

  @Override
  public void process(NumberedDocumentData ndd) throws IOException {
    if (last == null) {
      last = ndd;
    } else {
      // ensure that we have an ident
      assert ndd.identifier != null: "DiskNameReverseWriter can not write a null identifier.";
      assert Utility.compare(last.identifier, ndd.identifier) <= 0: "DiskNameReverseWriter wrong order.";
      if(Utility.compare(last.identifier, ndd.identifier) == 0){
        Logger.getLogger(this.getClass().getName()).info("WARNING: identical document names written to names.reverse index");
      }
    }
    
    writer.add(new GenericElement(
            Utility.fromString(ndd.identifier),
            Utility.fromLong(ndd.number)));

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
