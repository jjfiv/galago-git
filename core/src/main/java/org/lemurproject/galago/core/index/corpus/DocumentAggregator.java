// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.PseudoDocument;
import org.lemurproject.galago.core.index.disk.DiskBTreeWriter;
import org.lemurproject.galago.core.index.merge.CorpusMerger;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.xerial.snappy.SnappyInputStream;

@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key"})
public class DocumentAggregator implements KeyValuePair.KeyOrder.ShreddedProcessor {

  Counter docsIn, docsOut;
  DiskBTreeWriter writer;
  int documentNumber = 0;
  byte[] lastIdentifier = null;
  Map<String, PseudoDocument> bufferedDocuments;

  public DocumentAggregator(TupleFlowParameters parameters) throws IOException, FileNotFoundException {
    docsIn = parameters.getCounter("Documents in");
    docsOut = parameters.getCounter("Documents out");
    Parameters corpusParams = parameters.getJSON();
    // create a writer;
    corpusParams.set("writerClass", getClass().getName());
    corpusParams.set("readerClass", CorpusReader.class.getName());
    corpusParams.set("mergerClass", CorpusMerger.class.getName());
    corpusParams.set("pseudo", true);
    writer = new DiskBTreeWriter(parameters);
    bufferedDocuments = new HashMap<String, PseudoDocument>();
  }

  public void processKey(byte[] key) throws IOException {
    if (lastIdentifier == null
            || Utility.compare(key, lastIdentifier) != 0) {
      if (lastIdentifier != null) {
        write();
      }
      lastIdentifier = key;
    }
  }

  public void processTuple(byte[] value) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(value);
    Document document;
    try {
      ObjectInputStream input = new ObjectInputStream(new SnappyInputStream(stream));
      document = (Document) input.readObject();
      addToBuffer(document);
      if (docsIn != null) {
        docsIn.increment();
      }
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    }
  }

  private void addToBuffer(Document d) {
    if (!bufferedDocuments.containsKey(d.name)) {
      bufferedDocuments.put(d.name, new PseudoDocument(d));
    } else {
      bufferedDocuments.get(d.name).addSample(d);
    }
  }

  private Parameters emptyParameters = new Parameters();
  private void write() throws IOException {
    for (String nameKey : bufferedDocuments.keySet()) {
      ByteArrayOutputStream array = new ByteArrayOutputStream();
      PseudoDocument pd = bufferedDocuments.get(nameKey);
      pd.identifier = documentNumber;
      // This is a hack to make the document smaller
      if (pd.terms.size() > 1000000) {
	  pd.terms = pd.terms.subList(0, 1000000);
      }
      array.write(PseudoDocument.serialize(emptyParameters, pd));
      array.close();
      byte[] newKey = Utility.fromInt(pd.identifier);
      byte[] value = array.toByteArray();
      System.err.printf("Total stored document (%d) size: %d\n", pd.identifier, value.length);
      writer.add(new GenericElement(newKey, value));
      if (docsOut != null) {
        docsOut.increment();
      }
      ++documentNumber;
    }
    bufferedDocuments.clear();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("DocumentAggregator requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, handler);
  }

  public void close() throws IOException {
    write();
    writer.close();
  }
}
