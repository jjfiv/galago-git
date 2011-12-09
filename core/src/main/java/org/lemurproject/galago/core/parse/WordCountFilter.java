// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.types.WordCount", order = {"+word"})
@OutputClass(className = "org.lemurproject.galago.core.types.WordCount", order = {"+word"})
@Verified
public class WordCountFilter extends StandardStep<WordCount, WordCount> {

  private long minThreshold = 2;

  public WordCountFilter(TupleFlowParameters p) {
    minThreshold = p.getJSON().get("minThreshold", minThreshold);
  }

  public void process(WordCount wordCount) throws IOException {
    if(wordCount.count >= minThreshold){
      processor.process(wordCount);
    }
  }
}
