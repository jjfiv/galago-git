/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.window;

import java.io.IOException;
import org.lemurproject.galago.core.types.NumberWordCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.NumberWordCount", order = {"+word", "+document"})
@OutputClass(className = "org.lemurproject.galago.core.types.NumberWordCount", order = {"+word", "+document"})
public class ReduceNumberWordCount extends StandardStep<NumberWordCount, NumberWordCount>
        implements NumberWordCount.Source {

  NumberWordCount last;

  @Override
  public void process(NumberWordCount current) throws IOException {
    if (last == null) {
      last = current;

    } else if ((Utility.compare(last.word, current.word) == 0)
            && (Utility.compare(last.document, current.document) == 0)) {
      last.count += current.count;

    } else {
      processor.process(last);
      last = current;
    }
  }

  @Override
  public void close() throws IOException {
    if (last != null) {
      processor.process(last);
    }
    processor.close();
  }
}
