/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class DumpTermStatisticsFn extends AppFunction {

  @Override
  public String getName() {
    return "dump-term-stats";
  }

  @Override
  public String getHelpString() {
    return "galago dump-term-stats <index-part> \n\n"
            + "  Dumps <term> <frequency> <document count> statsistics from the"
            + " the specified index part.\n";
  }

  @Override
  public void run(String[] args, PrintStream output)
          throws Exception {
    IndexPartReader reader = DiskIndex.openIndexPart(args[1]);
    KeyIterator iterator = reader.getIterator();
    ScoringContext sc = new ScoringContext();
    while (!iterator.isDone()) {
      MovableCountIterator mci = (MovableCountIterator) iterator.getValueIterator();
      mci.setContext(sc);
      long frequency = 0;
      long documentCount = 0;
      while (!mci.isDone()) {
        if (mci.hasMatch(mci.currentCandidate())) {
          frequency += mci.count();
          documentCount++;
        }
        mci.movePast(mci.currentCandidate());
      }
      output.printf("%s\t%d\t%d\n", iterator.getKeyString(), frequency, documentCount);
      iterator.nextKey();
    }
    reader.close();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    String indexPath = p.getString("indexPath");
    run(new String[]{"", indexPath}, output);
  }
}