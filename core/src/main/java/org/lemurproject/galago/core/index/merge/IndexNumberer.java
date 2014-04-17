/*
 *  BSD License (http://lemurproject.org/galago-license)
 */

package org.lemurproject.galago.core.index.merge;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.Verified;

import java.io.IOException;
import java.util.List;

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
    for(String inputIndex : (List<String>) parameters.getJSON().getList("inputPath")) {
      processor.process(new DocumentSplit(inputIndex,"",new byte[0],new byte[0],i,total));
      i++;
    }

    processor.close();
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }
}
