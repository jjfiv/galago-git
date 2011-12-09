// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.WordCount")
public class WordCounter extends StandardStep<Document, WordCount> {

  HashSet<String> filterWords;

  public WordCounter(TupleFlowParameters parameters) throws IOException {
    String filename = parameters.getJSON().get("filter", (String) null);
    if (filename != null) {
      filterWords = Utility.readFileToStringSet(new File(filename));
    } else {
      filterWords = null;
    }
  }

  public void process(Document document) throws IOException {
    List<String> tokens = document.terms;
    ArrayList<WordCount> wordCounts = new ArrayList();

    for (String t : tokens) {
      if (t != null) {
        if ((filterWords == null)
                || (!filterWords.contains(t))) {
          wordCounts.add(new WordCount(Utility.fromString(t), 1, 1));
        }
      }
    }

    Collections.sort(wordCounts, new WordCount.WordOrder().lessThan());

    WordCount last = null;

    for (WordCount wc : wordCounts) {
      if (last == null) {
        last = wc;
      } else if (Utility.compare(wc.word, last.word) == 0) {
        last.count += wc.count;
      } else {
        processor.process(last);
        last = wc;
      }
    }

    if (last != null) {
      processor.process(last);
    }
  }
}
