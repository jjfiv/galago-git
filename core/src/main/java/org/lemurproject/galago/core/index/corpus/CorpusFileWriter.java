// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.disk.IndexWriter;
import org.lemurproject.galago.core.index.merge.CorpusMerger;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 * Writes document text and metadata to an index file.  The output files
 * are in '.corpus' format, which can be fed to UniversalParser as an input
 * to indexing.  The '.corpus' format is also convenient for quickly
 * finding individual documents.
 * 
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
public class CorpusFileWriter implements Processor<Document> {

  IndexWriter writer;
  Counter documentsWritten;
  boolean compressed;

  public CorpusFileWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    compressed = parameters.getJSON().get("compressed", true);

    // create a writer;
    Parameters p = new Parameters();
    p.set("compressed", parameters.getJSON().get("compressed",true));
    p.set("writerClass", getClass().getName());
    p.set("readerClass", CorpusReader.class.getName());
    p.set("mergerClass", CorpusMerger.class.getName());

    writer = new IndexWriter(parameters.getJSON().getString("filename"), p);
    documentsWritten = parameters.getCounter("Documents Written");
  }

  public void close() throws IOException {
    writer.close();
  }

  public void process(Document document) throws IOException {
    writer.add(new GenericElement(Utility.fromInt(document.identifier), Document.serialize(document, compressed)));
    if (documentsWritten != null) {
      documentsWritten.increment();
    }
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("DocumentIndexWriter requires an 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, handler);
  }
}
