// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import java.util.Arrays;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.types.WordCount", order = {"+word"})
@OutputClass(className = "org.lemurproject.galago.core.types.WordCount", order = {"+word"})
@Verified
public class WordCountReducer extends StandardStep<WordCount, WordCount> implements
        WordCount.Source {

  private WordCount last = null;

  @Override
  public void process(WordCount wordCount) throws IOException {
    if (last == null) {
      last = wordCount;
    } else if (Arrays.equals(wordCount.word, last.word)) {
      last.collectionFrequency += wordCount.collectionFrequency;
      last.documentCount += wordCount.documentCount;
      last.maxDocumentFrequency = Math.max(last.maxDocumentFrequency, wordCount.maxDocumentFrequency);
    } else {
      processor.process(last);
      last = wordCount;
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
