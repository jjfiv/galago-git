/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.ExNihiloSource;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@OutputClass(className = "java.lang.String")
public class FileLineParser implements ExNihiloSource<String> {

  public Processor<String> processor;
  Parameters p;
  Counter lines;

  public FileLineParser(TupleFlowParameters parameters) {
    p = parameters.getJSON();
    lines = parameters.getCounter("File Lines Read");
  }

  @Override
  public void run() throws IOException {
    BufferedReader reader;
    for (String f : (List<String>) p.getList("inputPath")) {
      DocumentSplit split = new DocumentSplit();
      split.fileName = f;
      split.isCompressed = ( f.endsWith(".gz") || f.endsWith(".bz") );
      reader = DocumentStreamParser.getBufferedReader( split );
      String line;
      while (null != (line = reader.readLine())) {
        if(lines != null) lines.increment();

        if (line.startsWith("#")) {
          continue;
        }
        processor.process(line);
      }
      reader.close();
    }
    processor.close();
  }

  @Override
  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }
}
