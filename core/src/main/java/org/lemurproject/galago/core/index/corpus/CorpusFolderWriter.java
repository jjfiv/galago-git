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
 * Writes documents to a file
 *  - new output file is created in the folder specified by "filename"
 *  - document.name -> output-file, byte-offset is passed on
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class CorpusFolderWriter implements Processor<Document>, Source<KeyValuePair> {

  boolean compressed;
  SplitIndexValueWriter writer;

  public CorpusFolderWriter(TupleFlowParameters parameters) throws IOException, IncompatibleProcessorException {
    compressed = parameters.getJSON().get("compressed", true);

    // create a writer;
    Parameters p = new Parameters();
    p.set("compressed", parameters.getJSON().get("compressed",true));
    p.set("writerClass", getClass().getName());
    p.set("readerClass", CorpusReader.class.getName());
    p.set("mergerClass", CorpusMerger.class.getName());
    p.set("filename", parameters.getJSON().getString("filename"));
    writer = new SplitIndexValueWriter(parameters);
    // note that the setProcessor function needs to be modified!
  }

  public void process(Document document) throws IOException {
    writer.add(new GenericElement(Utility.fromInt(document.identifier), Document.serialize(document, compressed)));
  }

  public void close() throws IOException {
    writer.close();
  }

  public void setProcessor(Step next) throws IncompatibleProcessorException {
    Linkage.link(writer, next);
  }
}
