// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.window;

import java.io.IOException;
import org.lemurproject.galago.core.types.TextFeature;
import org.lemurproject.galago.tupleflow.Counter;

import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * WindowFilter uses a filter that contains locations of
 * potentially frequent windows
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.window.Window")
@OutputClass(className = "org.lemurproject.galago.core.window.Window")
public class WindowFilter extends StandardStep<Window, Window> {
  TypeReader<TextFeature> filterStream;
  TextFeature filterHead;

  long dropCount = 0;
  long totalCount = 0;

  Counter dropped;
  Counter passed;

  public WindowFilter(TupleFlowParameters parameters) throws IOException {
    String filterStreamName = parameters.getJSON().getString("filterStream");
    filterStream = parameters.getTypeReader( filterStreamName );
    filterHead = filterStream.read();

    dropped = parameters.getCounter("Windows Dropped");
    passed = parameters.getCounter("Windows Passed");
  }
  
  public void process(Window w) throws IOException {
    totalCount++;

    // skip filterstream to the correct file
    while ((filterHead != null) &&
        (filterHead.file < w.file)){
      filterHead = filterStream.read();
    }
    
    // skip filterstream to the correct filePosition
    while ((filterHead != null) && 
        (filterHead.file == w.file) &&
        (filterHead.filePosition < w.filePosition)){
      filterHead = filterStream.read();
    }

    // if this window is in the correct position -- process it
    if ((filterHead != null) && 
        (filterHead.file == w.file) &&
        (filterHead.filePosition == w.filePosition)){
      processor.process( w );
      if (passed != null) passed.increment();
    } else {
      // otherwise ignore it
      dropCount++;
      if(dropped != null) dropped.increment();
    }
  }
  
  public void close() throws IOException {
    System.out.println("Dropped " + dropCount + " of " + totalCount);
    processor.close();
  }
}
