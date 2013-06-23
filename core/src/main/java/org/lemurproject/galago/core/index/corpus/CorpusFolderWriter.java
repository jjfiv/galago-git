// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.merge.CorpusMerger;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Source;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

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

  Parameters corpusParams;
  SplitBTreeValueWriter writer;

  public CorpusFolderWriter(TupleFlowParameters parameters) throws IOException, IncompatibleProcessorException {
    corpusParams = parameters.getJSON();
    // create a writer;
    corpusParams.set("writerClass", getClass().getName());
    corpusParams.set("readerClass", CorpusReader.class.getName());
    corpusParams.set("mergerClass", CorpusMerger.class.getName());
    writer = new SplitBTreeValueWriter(parameters);
  }

  @Override
  public void process(Document document) throws IOException {
    writer.add(new GenericElement(Utility.fromLong(document.identifier),
            Document.serialize(document)));
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
