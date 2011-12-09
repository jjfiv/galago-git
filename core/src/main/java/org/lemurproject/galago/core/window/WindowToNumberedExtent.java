// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.window;

import java.io.IOException;

import org.lemurproject.galago.core.types.NumberedExtent;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * 
 * Converts Ngram objects to NumberWordPosition objects
 * Some data is discarded
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.window.Window")
@OutputClass(className = "org.lemurproject.galago.core.types.NumberedExtent")
public class WindowToNumberedExtent extends StandardStep<Window, NumberedExtent> {

  public void process(Window window) throws IOException {
    processor.process(
       new NumberedExtent(window.data, window.document, window.begin, window.end));
  }
}
