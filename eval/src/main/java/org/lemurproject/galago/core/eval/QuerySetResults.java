/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval;

import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamCreator;
import org.lemurproject.galago.utility.compare.NaturalOrderComparator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * This class holds retrieval results for a set of queries. Each query
 * corresponds to a QueryResult which contains the list of returned
 * ScoredDocuments
 *
 * @author sjh
 * @author jdalton
 */
public class QuerySetResults {

    private String name;
    private Map<String, QueryResults> querySetResults = new TreeMap<>(new NaturalOrderComparator());

    public QuerySetResults(Map<String, List<EvalDoc>> results) {
        name = "results";
        for (String query : results.keySet()) {
            // There is a possibility that an immutable list could be 
            // passed in so we make a copy of the List so it can be sorted.
            ArrayList<EvalDoc> rankedList = new ArrayList<>(results.get(query));
            check(rankedList);
            Collections.sort(rankedList, SimpleEvalDoc.byAscendingRank);
            querySetResults.put(query, new QueryResults(query, rankedList));
        }
    }

    public QuerySetResults(String filename) throws IOException {
        name = filename;
        loadRanking(filename);
    }

    public Iterable<String> getQueryIterator() {
        return querySetResults.keySet();
    }

    public QueryResults get(String query) {
        return querySetResults.get(query);
    }

    public String getName() {
        return name;
    }

    /**
     * Reads in a TREC ranking file.
     *
     * @param filename The filename of the ranking file.
     */
    private void loadRanking(String filename) throws IOException {
        // open file
        try (BufferedReader in = new BufferedReader(new InputStreamReader(StreamCreator.openInputStream(filename), "UTF-8"), 256 * 1024)) {

            int index = 1;
            String line;
            TreeMap<String, List<EvalDoc>> ranking = new TreeMap<>();

            for (; ; index++) {
                line = in.readLine();
                if (line == null) {
                    break;
                }

                try {
                    // 1 Q0 WSJ880711-0086 39 -3.05948 Exp
                    String[] cols = line.split("\\s+");

                    String queryNumber = cols[0];
                    //String unused = cols[1];
                    String docno = cols[2];
                    String rank = cols[3];
                    String score = cols[4];
                    //String runtag = cols[5];

                    EvalDoc document = new SimpleEvalDoc(docno, Integer.parseInt(rank), Double.parseDouble(score));

                    if (!ranking.containsKey(queryNumber)) {
                        ranking.put(queryNumber, new ArrayList<EvalDoc>());
                    }
                    ranking.get(queryNumber).add(document);
                } catch (Exception err) {
                    throw new IllegalArgumentException("Failed to parse qrels " + filename + ":" + index, err);
                }
            }

            // ensure sorted order by rank
            for (String query : ranking.keySet()) {
                List<EvalDoc> documents = ranking.get(query);
                Collections.sort(documents, SimpleEvalDoc.byAscendingRank);
                querySetResults.put(query, new QueryResults(query, documents));
            }
        }
    }

    /**
     * Given a list of queries this function ensures that all queries exist in
     * the query set - avoids problems where no documents are returned
     */
    public void ensureQuerySet(List<Parameters> queries) {
        for (Parameters query : queries) {
            if (query.isString("number")) {
                String num = query.getString("number");
                if (!querySetResults.containsKey(num)) {
                    querySetResults.put(num, new QueryResults(num, new ArrayList<EvalDoc>()));
                }
            }
        }
    }

    private void check(ArrayList<EvalDoc> rankedList) {
        for (EvalDoc sdoc : rankedList) {
            assert (sdoc.getRank() != 0) : "Ranked list contains a document with zero rank. Ranked lists must start from 1.";
        }
    }
}
