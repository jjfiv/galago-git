// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.File;
import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.disk.DiskBTreeWriter;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 * Split index key writer
 *  - Index is a mapping from byte[] to byte[]
 *
 *  - allows values to be written out of order to a set of files
 *  - a unified ordered key structure should be kept in a folder
 *    with these value files, as created by SplitBTreeKeyWriter
 *  - SplitIndexReader will read this data
 *
 *  This class if useful for writing a corpus structure
 *  - documents can be written to disk in any order
 *  - the key structure allows the documents to be found quickly
 *  - class is more efficient if the
 *    documents are inserted in sorted order
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class SplitBTreeKeyWriter implements Processor<KeyValuePair> {

  DiskBTreeWriter writer;
  private Counter keyCounter;

  public SplitBTreeKeyWriter(TupleFlowParameters parameters) throws IOException {
    String file = parameters.getJSON().getString("filename") + File.separator + "split.keys";
    Utility.makeParentDirectories(file);
    writer = new DiskBTreeWriter(file, parameters.getJSON());
    keyCounter = parameters.getCounter("Document Keys Written");
  }

  @Override
  public void process(KeyValuePair object) throws IOException {
    writer.add(new GenericElement(object.key, object.value));
    if (keyCounter != null) {
      keyCounter.increment();
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("DocumentIndexWriter requires an 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableDirectory(index, handler);
  }
}
