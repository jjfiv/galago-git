/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

import java.io.PrintStream;

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

        if (args.length <= 1) {
            output.println(getHelpString());
            return;
        }

        ScoringContext sc = new ScoringContext();
        IndexPartReader reader = DiskIndex.openIndexPart(args[1]);
        KeyIterator iterator = reader.getIterator();
        while (!iterator.isDone()) {
            CountIterator mci = (CountIterator) iterator.getValueIterator();
            long frequency = 0;
            long documentCount = 0;
            while (!mci.isDone()) {
                sc.document = mci.currentCandidate();
                if (mci.hasMatch(sc)) {
                    frequency += mci.count(sc);
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
