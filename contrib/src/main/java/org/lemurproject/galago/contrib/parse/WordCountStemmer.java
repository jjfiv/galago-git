/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.parse;

import java.io.IOException;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.WordCount")
@OutputClass(className = "org.lemurproject.galago.core.types.WordCount")
public class WordCountStemmer extends StandardStep<WordCount, WordCount> {

  Stemmer stemmer;

  public WordCountStemmer(TupleFlowParameters p) throws Exception {
    stemmer = (Stemmer) Class.forName(p.getJSON().getString("stemmer")).newInstance();
  }

  @Override
  public void process(WordCount wc) throws IOException {
    wc.word = Utility.fromString(stemmer.stem(Utility.toString(wc.word)));
    processor.process(wc);
  }
}
