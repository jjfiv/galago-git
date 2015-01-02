package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

import java.io.PrintStream;

/**
 * @author jfoley
 */
public class DumpLengthsFn extends AppFunction {
  @Override
  public String getName() {
    return "dump-lengths";
  }

  @Override
  public String getHelpString() {
    return "galago dump-lengths --index=[indexPath]\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.containsKey("index")) {
      output.println(this.getHelpString());
      return;
    }

    DiskIndex index = new DiskIndex(p.getString("index"));
    LengthsIterator lengthsItr = index.getLengthsIterator();

    ScoringContext sc = new ScoringContext();

    while (!lengthsItr.isDone()) {
      long docId = lengthsItr.currentCandidate();
      sc.document = docId;
      lengthsItr.syncTo(docId);
      int docLen = lengthsItr.length(sc);
      output.println(docId + "\t" + docLen);
      lengthsItr.movePast(docId);
    }
  }
}
