// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.window;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
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

  long debug_count = 0;
  long debug_total_count = 0;
  int threshold;
  boolean threshdf;
  LinkedList<NumberedExtent> current;
  boolean currentPassesThreshold;

  Counter discards;
  Counter passing;

  public NumberedExtentThresholder(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException {
    threshold = (int) parameters.getJSON().getLong("threshold");
    threshdf = parameters.getJSON().getBoolean("threshdf");
    current = new LinkedList();

    discards = parameters.getCounter("Discarded Extents");
    passing = parameters.getCounter("Passed Extents");
  }

  public void process(NumberedExtent ne) throws IOException {
    debug_total_count++;

    if ((current.size() > 0)
            && (Utility.compare(ne.extentName, current.peekFirst().extentName) == 0)) {
      current.offerLast(ne);
      emitExtents();
    } else {
      emitExtents();
      if(discards != null) discards.incrementBy( current.size() );
      current.clear();
      current.offerLast(ne);
      currentPassesThreshold = false;
    }
  }

  private void emitExtents() throws IOException {

    // if we have more than threshold df
    if (threshdf) {
      HashSet<Long> docs = new HashSet();
      for(NumberedExtent e : current){
        docs.add(e.number);
      }
      if(docs.size() >= threshold){
        currentPassesThreshold = true;
      }
    } else {
      if (current.size() >= threshold) {
        currentPassesThreshold = true;
      }
    }

    // now actually emit Extents
    if (currentPassesThreshold) {
      while (current.size() > 0) {
        processor.process(current.pollFirst());
        if(passing != null) passing.increment();
      }
    }
  }

  public void close() throws IOException {
    emitExtents();
    processor.close();
  }
}
