// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import org.lemurproject.galago.core.corpus.DocumentSerializer;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.merge.CorpusMerger;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;

/**
 * Writes documents to a file - new output file is created in the folder
 * specified by "filename" - document.identifier -> output-file, byte-offset is
 * passed on
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class CorpusFolderWriter implements Processor<Document>, Source<KeyValuePair> {

  final DocumentSerializer serializer;
  final Parameters corpusParams;
  final SplitBTreeValueWriter writer;

  public CorpusFolderWriter(TupleFlowParameters parameters) throws IOException, IncompatibleProcessorException {
    corpusParams = parameters.getJSON();
    // create a writer;
    corpusParams.set("writerClass", getClass().getName());
    corpusParams.set("readerClass", CorpusReader.class.getName());
    corpusParams.set("mergerClass", CorpusMerger.class.getName());
    writer = new SplitBTreeValueWriter(parameters);
    serializer = DocumentSerializer.instance(corpusParams);
    corpusParams.set("documentSerializerClass", serializer.getClass().getName());
  }

  @Override
  public void process(Document document) throws IOException {
    writer.add(new GenericElement(Utility.fromLong(document.identifier),
            serializer.toBytes(document)));
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public void setProcessor(Step next) throws IncompatibleProcessorException {
    Linkage.link(writer, next);
  }
}
