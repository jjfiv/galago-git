// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.window;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.LinkedList;

import org.lemurproject.galago.core.types.NumberWordCount;
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
@InputClass(className = "org.lemurproject.galago.core.types.NumberWordCount", order = {"+word", "+document"})
@OutputClass(className = "org.lemurproject.galago.core.types.NumberWordCount", order = {"+word", "+document"})
public class NumberWordCountThresholder extends StandardStep<NumberWordCount, NumberWordCount>
        implements NumberWordCount.Source {

  long debug_count = 0;
  long debug_total_count = 0;
  int threshold;
  boolean threshdf;
  LinkedList<NumberWordCount> current;
  boolean currentPassesThreshold;

  Counter discards;
  Counter passing;

  public NumberWordCountThresholder(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException {
    threshold = (int) parameters.getJSON().getLong("threshold");
    threshdf = parameters.getJSON().getBoolean("threshdf");
    current = new LinkedList();

    discards = parameters.getCounter("Discarded Extents");
    passing = parameters.getCounter("Passed Extents");
  }

  public void process(NumberWordCount nwc) throws IOException {
    debug_total_count++;

    if ((current.size() > 0)
            && (Utility.compare(nwc.word, current.peekFirst().word) == 0)) {
      current.offerLast(nwc);
      emitExtents();
    } else {
      emitExtents();
      if(discards != null) discards.incrementBy( current.size() );
      current.clear();
      current.offerLast(nwc);
      currentPassesThreshold = false;
    }
  }

  private void emitExtents() throws IOException {

    // if we have more than threshold df
    if (threshdf) {
      HashSet<Integer> docs = new HashSet();
      for(NumberWordCount e : current){
        docs.add(e.document);
      }
      if(docs.size() >= threshold){
        currentPassesThreshold = true;
      }
    } else {
      int totalCount = 0;
      for(NumberWordCount e : current){
        totalCount += e.count;
      }
      if (totalCount >= threshold) {
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
