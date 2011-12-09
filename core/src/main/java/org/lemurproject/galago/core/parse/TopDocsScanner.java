// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.disk.DiskLengthsReader;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.CountValueIterator;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.core.types.TopDocsEntry;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.FileSource;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;

/**
 *
 * @author irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key"})
@OutputClass(className = "org.lemurproject.galago.core.types.TopDocsEntry", order = {"+word", "-probability", "+document"})
public class TopDocsScanner extends StandardStep<KeyValuePair, TopDocsEntry> {

  private Logger LOG = Logger.getLogger(getClass().toString());

  public static class NWPComparator implements Comparator<TopDocsEntry> {

    public int compare(TopDocsEntry a, TopDocsEntry b) {
      int result = (a.probability > b.probability) ? 1
              : ((a.probability < b.probability) ? -1 : 0);
      if (result != 0) {
        return result;
      }
      return (a.document - b.document);
    }
  }
  protected int size;
  protected long minlength;
  protected long count;
  Counter counter;
  PriorityQueue<TopDocsEntry> topdocs;
  PositionIndexReader partReader;
  DiskLengthsReader.KeyIterator docLengths;
  DiskLengthsReader docReader;
  CountValueIterator extentIterator;
  TopDocsEntry tde;

  public TopDocsScanner(TupleFlowParameters parameters) throws Exception {
    size = (int) parameters.getJSON().get("size", Integer.MAX_VALUE);
    minlength = parameters.getJSON().get("minlength", Long.MAX_VALUE);
    topdocs = new PriorityQueue<TopDocsEntry>(size, new NWPComparator());
    String indexLocation = parameters.getJSON().getString("directory");
    docReader = new DiskLengthsReader(indexLocation
            + File.separator + "lengths");
    docLengths = docReader.getIterator();
    partReader = new PositionIndexReader(DiskIndex.getPartPath(indexLocation, parameters.getJSON().getString("part")));
    counter = parameters.getCounter("lists scanned");
  }

  @Override
  public void process(KeyValuePair object) throws IOException {
    if (counter != null) {
      counter.increment();
    }
    // Get out posting list
    count = 0;
    topdocs.clear();
    extentIterator = partReader.getTermCounts(Utility.toString(object.key));
    count = extentIterator.totalEntries();
    if (count < minlength) {
      return; //short-circuit out
    }

    count = 0; // need to reset b/c we're going to count anyhow.

    // And iterate
    docLengths.reset();
    while (!extentIterator.isDone()) {
      count++;
      docLengths.skipToKey(extentIterator.currentCandidate());
      assert (docLengths.getCurrentIdentifier() == extentIterator.currentCandidate());
      int length = docLengths.getCurrentLength();
      double probability = (0.0 + extentIterator.count())
              / (0.0 + length);
      tde = new TopDocsEntry();
      tde.document = extentIterator.currentCandidate();
      tde.count = extentIterator.count();
      tde.doclength = length;
      tde.probability = probability;
      topdocs.add(tde);

      // Keep it trimmed
      if (topdocs.size() > size) {
        topdocs.poll();
      }
      extentIterator.next();
    }

    // skip if it's too small
    if (count < minlength) {
      topdocs.clear();
      return;
    }

    while (topdocs.size() > size) {
      topdocs.poll();
    }

    // Now emit based on our top docs (have to reverse first)
    ArrayList<TopDocsEntry> resort = new ArrayList<TopDocsEntry>(topdocs);
    Collections.sort(resort, new Comparator<TopDocsEntry>() {

      public int compare(TopDocsEntry a, TopDocsEntry b) {
        //return (a.document - b.document);
        return (a.probability > b.probability ? -1 : (a.probability < b.probability ? 1 : 0));
      }
    });


    for (TopDocsEntry entry : resort) {
      entry.word = object.key;
      processor.process(entry);
    }
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    FileSource.verify(parameters, handler);
    if (!parameters.getJSON().containsKey("size")) {
      handler.addError("Need size.");
    }
    if (!parameters.getJSON().containsKey("minlength")) {
      handler.addError("Need minlength");
    }
    if (!parameters.getJSON().containsKey("part")) {
      handler.addError("Need index part");
    }
  }

  public void close() throws IOException {
    docReader.close();
    partReader.close();
    processor.close();
  }
}
