// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.window;

import gnu.trove.set.hash.TLongHashSet;
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

  long debug_pass_count = 0;
  long debug_total_count = 0;
  int threshold;
  boolean threshdf;
  byte[] currentFeature;
  LinkedList<NumberWordCount> currentBuffer;
  TLongHashSet docs;
  boolean currentPassesThreshold;
  
  Counter discards;
  Counter passing;

  public NumberWordCountThresholder(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException {
    threshold = (int) parameters.getJSON().getLong("threshold");
    threshdf = parameters.getJSON().getBoolean("threshdf");
    currentFeature = null;
    docs = new TLongHashSet();
    currentBuffer = new LinkedList();

    discards = parameters.getCounter("Discarded Extents");
    passing = parameters.getCounter("Passed Extents");
  }

  @Override
  public void process(NumberWordCount nwc) throws IOException {
    debug_total_count++;

    // first feature - record the feature + store the tf in the buffer
    if (currentFeature == null) {
      currentFeature = nwc.word;
      currentBuffer.offerLast(nwc);
      // no point emitting here - threshold should be > 1

    } else if (Utility.compare(nwc.word, currentFeature) == 0) {
      currentBuffer.offerLast(nwc);
      emitExtents();

    } else {
      //emitExtents();
      if (discards != null) {
        discards.incrementBy(currentBuffer.size());
      }
      currentBuffer.clear();

      // now prepare for the next feature
      currentFeature = nwc.word;
      currentBuffer.offerLast(nwc);
      currentPassesThreshold = false;
    }
  }

  private void emitExtents() throws IOException {

    // if we have more than threshold df
    if (threshdf) {
      HashSet<Integer> docs = new HashSet();
      for(NumberWordCount e : currentBuffer){
        docs.add(e.document);
      }
      if(docs.size() >= threshold){
        currentPassesThreshold = true;
      }
    } else {
      int totalCount = 0;
      for(NumberWordCount e : currentBuffer){
        totalCount += e.count;
      }
      if (totalCount >= threshold) {
        currentPassesThreshold = true;
      }
    }

    // now actually emit Extents
    if (currentPassesThreshold) {
      while (currentBuffer.size() > 0) {
        processor.process(currentBuffer.pollFirst());
        if(passing != null) passing.increment();
      }
    }
  }

  public void close() throws IOException {
    // emitExtents();
    processor.close();
  }
}
