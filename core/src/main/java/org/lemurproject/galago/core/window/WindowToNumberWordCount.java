/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.window;

import java.io.IOException;
import org.lemurproject.galago.core.types.NumberWordCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.window.Window")
@OutputClass(className = "org.lemurproject.galago.core.types.NumberWordCount")
public class WindowToNumberWordCount extends StandardStep<Window, NumberWordCount> {

  @Override
  public void process(Window window) throws IOException {
    processor.process(new NumberWordCount(window.data, window.document, 1));
  }
}
