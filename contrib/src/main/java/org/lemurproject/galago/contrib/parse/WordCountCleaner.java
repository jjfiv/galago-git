/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.parse;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Pattern;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Parameters;
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
public class WordCountCleaner extends StandardStep<WordCount, WordCount> {

  HashMap<String, String> replacements;
  String exceptions;
  boolean stripPunct;
  boolean lower;

  public WordCountCleaner(TupleFlowParameters p) throws Exception {
    stripPunct = p.getJSON().get("stripPunct", true);
    lower = p.getJSON().get("lower", true);

    Parameters repls = p.getJSON().getMap("replacements");
    replacements = new HashMap();
    exceptions = "[^\\w";
    for (String key : repls.getKeys()) {
      replacements.put(key, repls.getString(key));
      exceptions += key;
    }
    exceptions += "]";
  }

  @Override
  public void process(WordCount wc) throws IOException {
    String t = Utility.toString(wc.word);
    if (lower) {
      t = t.toLowerCase();
    }
    if (stripPunct) {
      // remove all punctuation
      // NOTE: java 1.7 allows \W to match unicode chars correctly.
      t = Pattern.compile(exceptions).matcher(t).replaceAll("");
    }

    for (String key : replacements.keySet()) {
      t = t.replaceAll(key, replacements.get(key));
    }

    // empty strings should be removed
    if (!t.isEmpty()) {
      wc.word = Utility.fromString(t);
      processor.process(wc);
    }
  }
}
