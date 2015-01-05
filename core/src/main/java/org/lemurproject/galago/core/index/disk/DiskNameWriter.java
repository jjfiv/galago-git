// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.btree.format.TupleflowDiskBTreeWriter;
import org.lemurproject.galago.utility.btree.GenericElement;
import org.lemurproject.galago.core.index.merge.DocumentNameMerger;
import org.lemurproject.galago.core.types.DocumentNameId;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;

import java.io.IOException;

/**
 * Writes a btree mapping from document names to document numbers
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.DocumentNameId", order = {"+id"})
public class DiskNameWriter implements Processor<DocumentNameId> {

  TupleflowDiskBTreeWriter writer;
  DocumentNameId last = null;
  Counter documentNamesWritten;

  public DiskNameWriter(TupleFlowParameters parameters) throws IOException {
    documentNamesWritten = parameters.getCounter("Document Names Written");
    // make a folder
    String filename = parameters.getJSON().getString("filename");

    Parameters p = parameters.getJSON();
    p.set("writerClass", DiskNameWriter.class.getName());
    p.set("mergerClass", DocumentNameMerger.class.getName());
    p.set("readerClass", DiskNameReader.class.getName());

    writer = new TupleflowDiskBTreeWriter(filename, p);
  }

  @Override
  public void process(DocumentNameId ndd) throws IOException {
    if (last == null) {
      last = ndd;
    }

    assert last.id <= ndd.id;
    assert ndd.name != null;

    writer.add(new GenericElement(Utility.fromLong(ndd.id), ndd.name));
    documentNamesWritten.increment();
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
