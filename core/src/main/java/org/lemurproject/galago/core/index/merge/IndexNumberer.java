/*
 *  BSD License (http://lemurproject.org/galago-license)
 */

package org.lemurproject.galago.core.index.merge;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.util.DocumentSplitFactory;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.Verified;

import java.io.IOException;

/**
 *
 * @author sjh
 */
@Verified
@OutputClass( className="org.lemurproject.galago.core.types.DocumentSplit", order={"+fileId"} )
public class IndexNumberer implements ExNihiloSource<DocumentSplit> {
  public Processor<DocumentSplit> processor;
  TupleFlowParameters parameters;
  
  public IndexNumberer(TupleFlowParameters parameters){
    this.parameters = parameters;
  }

  public void run() throws IOException {
    int i = 0 ;
    int total = parameters.getJSON().getList("inputPath").size();
    for(String inputIndex : parameters.getJSON().getList("inputPath", String.class)) {
      DocumentSplit split = DocumentSplitFactory.numberedFile(inputIndex, i, total);
      processor.process(split);
      i++;
    }

    processor.close();
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }
}
