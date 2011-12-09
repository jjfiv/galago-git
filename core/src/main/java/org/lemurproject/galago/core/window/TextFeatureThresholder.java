// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.window;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import org.lemurproject.galago.core.types.TextFeature;
import org.lemurproject.galago.tupleflow.Counter;

import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * Discards NumberWordPosition items that contain words that
 * occur less than <threshold> times within the corpus.
 *
 * TODO: The option to throw away features based on idf.
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.TextFeature", order = {"+feature"})
@OutputClass(className = "org.lemurproject.galago.core.types.TextFeature", order = {"+feature"})
public class TextFeatureThresholder extends StandardStep<TextFeature, TextFeature>
        implements TextFeature.Source {

  long debug_total_count = 0;
  int threshold;
  LinkedList<TextFeature> current;
  boolean currentPassesThreshold;

  Counter passing;
  Counter notPassing;

  public TextFeatureThresholder(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException {
    threshold = (int) parameters.getJSON().getLong("threshold");
    current = new LinkedList();
    currentPassesThreshold = false;

    passing = parameters.getCounter("Passed Features");
    notPassing = parameters.getCounter("Discarded Features");
  }

  public void process(TextFeature tf) throws IOException {
    debug_total_count++;

    if ((current.size() > 0)
            && (Utility.compare(tf.feature, current.peekFirst().feature) == 0)) {
      current.offerLast(tf);

      emitExtents();
    } else {
      // try to emit any passing extents
      emitExtents();
      if (notPassing != null) notPassing.incrementBy( current.size() );
      current.clear();
      current.offerLast(tf);
      currentPassesThreshold = false;
    }
  }

  public void close() throws IOException {
    // try to emit any passing extents
    emitExtents();
    processor.close();
  }

  private void emitExtents() throws IOException {

    if (current.size() >= threshold) {
      currentPassesThreshold = true;
    }
    if (currentPassesThreshold) {
      while (current.size() > 0) {
        processor.process(current.poll());
        if (passing != null) passing.increment();
      }
    }
  }
}
