// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.index.disk.DiskBTreeWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 * Almost complete class. Makes assumptions that you ultimately want to write
 * every incoming item to an output file, so it handles as much boilerplate as
 * possible. A canonical use case is the DocumentContentWriter, which is used to
 * write the corpus in Galago 2.0.
 *
 * Only thing to really implement is the prepare method.
 *
 * @author irmarc
 */
public abstract class KeyValueWriter<T> implements Processor<T> {

  protected DiskBTreeWriter writer;
  protected Counter elementsWritten;

  public KeyValueWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    this(parameters, "Documents written");
  }

  public KeyValueWriter(TupleFlowParameters parameters, String text) throws FileNotFoundException, IOException {
    writer = new DiskBTreeWriter(parameters);
    elementsWritten = parameters.getCounter(text);
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("KeyValueWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, store);
  }

  protected abstract GenericElement prepare(T item) throws IOException;

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public void process(T i) throws IOException { 
    GenericElement e = prepare(i);
    if (e != null) {
      writer.add(e);
      if (elementsWritten != null) {
        elementsWritten.increment();
      }
    }
  }
}
