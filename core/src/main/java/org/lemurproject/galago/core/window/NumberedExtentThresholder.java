// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.window;

import gnu.trove.set.hash.TLongHashSet;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;

import org.lemurproject.galago.core.types.NumberedExtent;
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
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.NumberedExtent", order = {"+extentName", "+number", "+begin"})
@OutputClass(className = "org.lemurproject.galago.core.types.NumberedExtent", order = {"+extentName", "+number", "+begin"})
public class NumberedExtentThresholder extends StandardStep<NumberedExtent, NumberedExtent>
        implements NumberedExtent.Source {

  long debug_pass_count = 0;
  long debug_total_count = 0;
  int threshold;
  boolean threshdf;
  byte[] currentFeature;
  LinkedList<NumberedExtent> currentBuffer;
  TLongHashSet docs;
  boolean currentPassesThreshold;
  Counter discards;
  Counter passing;

  public NumberedExtentThresholder(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException {
    threshold = (int) parameters.getJSON().getLong("threshold");
    threshdf = parameters.getJSON().getBoolean("threshdf");
    currentBuffer = new LinkedList();
    currentFeature = null;
    docs = new TLongHashSet();

    discards = parameters.getCounter("Discarded Extents");
    passing = parameters.getCounter("Passed Extents");
  }

  @Override
  public void process(NumberedExtent ne) throws IOException {
    debug_total_count++;

    // first feature - record the feature + store the tf in the buffer
    if (currentFeature == null) {
      currentFeature = ne.extentName;
      currentBuffer.offerLast(ne);
      // no point emitting here - threshold should be > 1

    } else if (Utility.compare(ne.extentName, currentFeature) == 0) {
      currentBuffer.offerLast(ne);
      emitExtents();

    } else {
      emitExtents();
      if (discards != null) {
        discards.incrementBy(currentBuffer.size());
      }
      currentBuffer.clear();

      // now prepare for the next feature
      currentFeature = ne.extentName;
      currentBuffer.offerLast(ne);
      currentPassesThreshold = false;
    }
  }

  private void emitExtents() throws IOException {

    // if we have more than threshold df
    if (threshdf) {
      docs.clear();
      for (NumberedExtent e : currentBuffer) {
        docs.add(e.number);
      }
      if (docs.size() >= threshold) {
        currentPassesThreshold = true;
      }
    } else {
      if (currentBuffer.size() >= threshold) {
        currentPassesThreshold = true;
      }
    }

    // now actually emit Extents
    if (currentPassesThreshold) {
      while (currentBuffer.size() > 0) {
        processor.process(currentBuffer.pollFirst());
        debug_pass_count++;
        if (passing != null) {
          passing.increment();
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    emitExtents();
    processor.close();
  }
}
