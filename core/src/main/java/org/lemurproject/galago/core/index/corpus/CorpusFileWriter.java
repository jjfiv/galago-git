// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import org.lemurproject.galago.core.corpus.DocumentSerializer;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.btree.format.DiskBTreeWriter;
import org.lemurproject.galago.core.index.merge.CorpusMerger;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;

import java.io.IOException;

/**
 * Writes document text and metadata to an index file. The output files are in
 * '.corpus' format, which can be fed to UniversalParser as an input to
 * indexing. The '.corpus' format is also convenient for quickly finding
 * individual documents.
 *
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
public class CorpusFileWriter implements Processor<Document> {

  final DocumentSerializer serializer;
  final Parameters corpusParams;
  final DiskBTreeWriter writer;
  final Counter documentsWritten;

  public CorpusFileWriter(TupleFlowParameters parameters) throws IOException {
    corpusParams = parameters.getJSON();
    // create a writer;
    corpusParams.set("writerClass", getClass().getName());
    corpusParams.set("readerClass", CorpusReader.class.getName());
    corpusParams.set("mergerClass", CorpusMerger.class.getName());
    writer = new DiskBTreeWriter(parameters.getJSON().getString("filename"), corpusParams);
    documentsWritten = parameters.getCounter("Documents Written");
    serializer = DocumentSerializer.instance(corpusParams);
    corpusParams.set("documentSerializerClass", serializer.getClass().getName());
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public void process(Document document) throws IOException {
    writer.add(new GenericElement(Utility.fromLong(document.identifier), serializer.toBytes(document)));
    documentsWritten.increment();
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("DocumentIndexWriter requires an 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, store);
  }
}
