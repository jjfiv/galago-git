// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Reducer;
import org.lemurproject.galago.tupleflow.Source;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.types.WordCount", order = {"+word"})
@OutputClass(className = "org.lemurproject.galago.core.types.WordCount", order = {"+word"})
@Verified
public class WordCountReducer implements Processor<WordCount>, Source<WordCount>, Reducer<WordCount>,
        WordCount.Processor {

  public Processor<WordCount> processor;
  private WordCount last = null;
  private WordCount aggregate = null;
  private long totalTerms = 0;

  public void process(WordCount wordCount) throws IOException {
    if (last != null) {
      if ( ! Arrays.equals(wordCount.word, last.word)){
        flush();
      } else if (aggregate == null) {
        aggregate = new WordCount(last.word, last.count + wordCount.count,
                last.documents + wordCount.documents);
      } else {
        aggregate.count += wordCount.count;
        aggregate.documents += wordCount.documents;
      }
    }

    last = wordCount;
  }

  public void flush() throws IOException {
    if (last != null) {
      if (aggregate != null) {
        assert aggregate != null;
        processor.process(aggregate);
        totalTerms += aggregate.count;
      } else {
        assert last != null;
        processor.process(last);
        totalTerms += last.count;
      }

      aggregate = null;
    }
  }

  public void close() throws IOException {
    flush();
    processor.close();
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }

  // this won't work at the moment...
  public ArrayList<WordCount> reduce(List<WordCount> input) throws IOException {
    HashMap<byte[], WordCount> countObjects = new HashMap<byte[], WordCount>();

    for (WordCount wordCount : input) {
      WordCount original = countObjects.get(wordCount.word);

      if (original == null) {
        countObjects.put(wordCount.word, wordCount);
      } else {
        original.documents += wordCount.documents;
        original.count += wordCount.count;
      }
    }

    return new ArrayList<WordCount>(countObjects.values());
  }

  public long getTotalTerms() {
    return totalTerms;
  }

  public Class<WordCount> getInputClass() {
    return WordCount.class;
  }

  public Class<WordCount> getOutputClass() {
    return WordCount.class;
  }
}
