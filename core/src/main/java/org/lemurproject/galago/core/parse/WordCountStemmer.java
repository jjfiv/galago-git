// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.tartarus.snowball.ext.englishStemmer;

/**
 *
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.types.WordCount", order = {"+word"})
@OutputClass(className = "org.lemurproject.galago.core.types.WordCount")
@Verified
public class WordCountStemmer extends StandardStep<WordCount, WordCount> {
  englishStemmer stemmer = new englishStemmer();
  
  public void process(WordCount wordCount) throws IOException {
    stemmer.setCurrent(Utility.toString(wordCount.word));
    byte[] stem;
    if(stemmer.stem()){
      stem = Utility.fromString(stemmer.getCurrent());
    } else {
      stem = wordCount.word;
    }
    processor.process(new WordCount(stem,wordCount.count,wordCount.documents));
  }
}
