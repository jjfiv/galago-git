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
import org.lemurproject.galago.utility.CmpUtil;

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

  long debug_pass_count = 0;
  long debug_total_count = 0;
  int threshold;
  byte[] currentFeature;
  LinkedList<TextFeature> currentBuffer;
  boolean currentPassesThreshold;
  Counter passing;
  Counter notPassing;

  public TextFeatureThresholder(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException {
    threshold = (int) parameters.getJSON().getLong("threshold");
    currentFeature = null;
    currentBuffer = new LinkedList();
    currentPassesThreshold = false;

    passing = parameters.getCounter("Passed Features");
    notPassing = parameters.getCounter("Discarded Features");
  }

  @Override
  public void process(TextFeature tf) throws IOException {
    debug_total_count++;

    // first feature - record the feature + store the tf in the buffer
    if (currentFeature == null) {
      currentFeature = tf.feature;
      currentBuffer.offerLast(tf);
      // no point emitting here - threshold should be > 1

    } else if (CmpUtil.equals(tf.feature, currentFeature)) {
      currentBuffer.offerLast(tf);
      emitExtents();

    } else {
      if (notPassing != null) {
        notPassing.incrementBy(currentBuffer.size());
      }
      currentBuffer.clear();

      // now prepare for the next feature
      currentFeature = tf.feature;
      currentBuffer.offerLast(tf);
      currentPassesThreshold = false;
    }
  }

  @Override
  public void close() throws IOException {
    // try to emit any passing extents
    //emitExtents();
    processor.close();
  }

  private void emitExtents() throws IOException {

    if (currentBuffer.size() >= threshold) {
      currentPassesThreshold = true;
    }
    if (currentPassesThreshold) {
      while (currentBuffer.size() > 0) {
        debug_pass_count++;
        TextFeature tf = currentBuffer.pollFirst();
        // zero out the feature.
        tf.feature = new byte[0];
        processor.process(tf);
        if (passing != null) {
          passing.increment();
        }
      }
    }
  }
}
