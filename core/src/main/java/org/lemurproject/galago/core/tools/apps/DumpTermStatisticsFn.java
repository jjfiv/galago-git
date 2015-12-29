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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                + " the specified index part.\n"
                + " Multiple index parts can be separated by commas.\n";
    }

    @Override
    public void run(String[] args, PrintStream output)
            throws Exception {

        if (args.length <= 1) {
            output.println(getHelpString());
            return;
        }

        String[] paths = args[1].split(",");
        Map<String, Long> freq = new HashMap<>();
        Map<String, Long> docCount = new HashMap<>();

        for (String path : paths) {
            ScoringContext sc = new ScoringContext();
            IndexPartReader reader = DiskIndex.openIndexPart(path);
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
                //        output.printf("%s\t%d\t%d\n", iterator.getKeyString(), frequency, documentCount);
                String key = iterator.getKeyString();
                if (freq.containsKey(key)){
                    freq.put(key, freq.get(key) + frequency);
                    docCount.put(key, docCount.get(key) + documentCount);
                } else {
                    freq.put(key,   frequency);
                    docCount.put(key,  documentCount);
                }
                iterator.nextKey();
            }
            reader.close();
        }
        for(String key : freq.keySet()){
            output.printf("%s\t%d\t%d\n", key, freq.get(key), docCount.get(key));
        }   
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
        String indexPath = p.getString("indexPath");
        run(new String[]{"", indexPath}, output);
    }
}
