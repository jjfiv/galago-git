// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.merge.DocumentNameMerger;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Sorter;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;

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
@InputClass(className = "org.lemurproject.galago.core.types.NumberedDocumentData")
public class DiskNameWriter implements Processor<NumberedDocumentData> {

  Sorter<KeyValuePair> sorterFL;
  Sorter<KeyValuePair> sorterRL;
  NumberedDocumentData last = null;
  Counter documentNamesWritten = null;

  public DiskNameWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    documentNamesWritten = parameters.getCounter("Document Names Written");
    // make a folder
    String fileName = parameters.getJSON().getString("filename");

    Parameters pforward = new Parameters();
    pforward.copyFrom(parameters.getJSON());
    pforward.set("order", "forward");
    pforward.set("writerClass", DiskNameWriter.class.getName());
    pforward.set("mergerClass", DocumentNameMerger.class.getName());
    pforward.set("readerClass", DiskNameReader.class.getName());

    IndexWriterProcessor writerFL = new IndexWriterProcessor(fileName, pforward);

    Parameters preverse = pforward.clone();
    preverse.set("order", "backward");
    preverse.remove("mergerClass");
    IndexWriterProcessor writerRL = new IndexWriterProcessor(fileName + ".reverse", preverse);

    sorterFL = new Sorter<KeyValuePair>(new KeyValuePair.KeyOrder());
    sorterRL = new Sorter<KeyValuePair>(new KeyValuePair.KeyOrder());
    sorterFL.processor = writerFL;
    sorterRL.processor = writerRL;

  }

  public void process(int number, String identifier) throws IOException {
    byte[] docnum = Utility.fromInt(number);
    byte[] docname = Utility.fromString(identifier);

    // numbers -> names
    KeyValuePair btiFL = new KeyValuePair(docnum, docname);
    sorterFL.process(btiFL);

    // names -> numbers
    KeyValuePair btiRL = new KeyValuePair(docname, docnum);
    sorterRL.process(btiRL);

    if (documentNamesWritten != null) {
      documentNamesWritten.increment();
    }
  }

  public void process(NumberedDocumentData ndd) throws IOException {
    if (last == null) {
      last = ndd;
    }

    assert last.number <= ndd.number;
    assert last.identifier != null;
    process(ndd.number, ndd.identifier);
  }

  public void close() throws IOException {
    sorterFL.close();
    sorterRL.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("DocumentNameWriter requires a 'filename' parameter.");
      return;
    }
  }

  /*
   * Translates the Key Value Pairs to Generic elements + writes them to the index
   */
  private class IndexWriterProcessor implements Processor<KeyValuePair> {

    IndexWriter writer;

    public IndexWriterProcessor(String fileName, Parameters p) throws IOException {
      // default uncompressed index is fine
      writer = new IndexWriter(fileName, p);
    }

    public void process(KeyValuePair kvp) throws IOException {
      GenericElement element = new GenericElement(kvp.key, kvp.value);
      writer.add(element);
    }

    public void close() throws IOException {
      writer.close();
    }
  }
}
