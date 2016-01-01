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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author sjh, michaelz (added thread support)
 */
public class DumpTermStatisticsFn extends AppFunction {

    private class Counts {
        Long freq;
        Long docCount;

        Counts(Long freq, Long docCount){
            this.freq = freq;
            this.docCount = docCount;
        }
    }

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
    public void run(String[] args, PrintStream output) throws Exception {

        if (args.length <= 1) {
            output.println(getHelpString());
            return;
        }

        ArrayList<Thread> threads = new ArrayList<>();

        String[] paths = args[1].split(",");
        ConcurrentHashMap<String, Counts> freq = new ConcurrentHashMap<>();

        for (String path : paths) {
            // process each index part in its own thread, although this
            // probably would be better as a TupleFlow process.
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        ScoringContext sc = new ScoringContext();
                        IndexPartReader reader = DiskIndex.openIndexPart(path);
                        KeyIterator iterator = reader.getIterator();
                        while (!iterator.isDone()) {
                            CountIterator mci = (CountIterator) iterator.getValueIterator();
                            Long frequency = 0L;
                            Long documentCount = 0L;
                            while (!mci.isDone()) {
                                sc.document = mci.currentCandidate();
                                if (mci.hasMatch(sc)) {
                                    frequency += mci.count(sc);
                                    documentCount++;
                                }
                                mci.movePast(mci.currentCandidate());
                            }
                            String key = iterator.getKeyString();

                            Counts c = freq.putIfAbsent(key, new Counts(frequency, documentCount));

                            if (c != null){
                                final Long tmpFrequency = frequency;
                                final Long tmpDocumentCount = documentCount;
                                freq.compute(key,  (k, v) -> {v.docCount += tmpDocumentCount; v.freq += tmpFrequency; return v;});
                            }
                            iterator.nextKey();
                        }
                        reader.close();
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                }
            };
            threads.add(t);
            t.start();
        }

        // Wait for a finished list
        for (Thread t : threads) {
            t.join();
        }

        for(String key : freq.keySet()){
            output.printf("%s\t%d\t%d\n", key, freq.get(key).freq, freq.get(key).docCount);
        }
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
        String indexPath = p.getString("indexPath");
        run(new String[]{"", indexPath}, output);
    }
}
