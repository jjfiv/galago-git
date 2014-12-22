/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.util.DocumentSplitFactory;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;

import java.io.BufferedReader;
import java.io.IOException;

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
    for (String f : p.getAsList("inputPath", String.class)) {
      DocumentSplit split = DocumentSplitFactory.file(f);
      reader = DocumentStreamParser.getBufferedReader( split );
      String line;
      while (null != (line = reader.readLine())) {
        lines.increment();

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
