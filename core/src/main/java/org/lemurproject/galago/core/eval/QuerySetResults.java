/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval;

import org.lemurproject.galago.core.eval.stat.NaturalOrderComparator;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.lists.Ranked;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
    private Map<String, QueryResults> querySetResults = new TreeMap<String, QueryResults>(new NaturalOrderComparator());

    public QuerySetResults(Map<String, List<ScoredDocument>> results) {
        name = "results";
        for (String query : results.keySet()) {
            // There is a possibility that an immutable list could be 
            // passed in so we make a copy of the List so it can be sorted.
            ArrayList rankedList = new ArrayList<ScoredDocument>(results.get(query));
            check(rankedList);
            Collections.sort(rankedList, Ranked.byAscendingRank);
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
        BufferedReader in = new BufferedReader(new FileReader(filename), 256 * 1024);
        String line;
        TreeMap<String, List<ScoredDocument>> ranking = new TreeMap<String, List<ScoredDocument>>();

        while ((line = in.readLine()) != null) {
            int[] splits = splits(line, 6);

            // 1 Q0 WSJ880711-0086 39 -3.05948 Exp
            String queryNumber = line.substring(splits[0], splits[1]);
            String unused = line.substring(splits[2], splits[3]);
            String docno = line.substring(splits[4], splits[5]);
            String rank = line.substring(splits[6], splits[7]);
            String score = line.substring(splits[8], splits[9]);
            String runtag = line.substring(splits[10]);

            ScoredDocument document = new ScoredDocument(docno, Integer.parseInt(rank), Double.parseDouble(score));

            if (!ranking.containsKey(queryNumber)) {
                ranking.put(queryNumber, new ArrayList<ScoredDocument>());
            }
            ranking.get(queryNumber).add(document);
        }

        // ensure sorted order by rank
        for (String query : ranking.keySet()) {
            List<ScoredDocument> documents = ranking.get(query);
            Collections.sort(documents, Ranked.byAscendingRank);
            querySetResults.put(query, new QueryResults(query, documents));
        }

        in.close();
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
                    querySetResults.put(num, new QueryResults(num, new ArrayList<ScoredDocument>()));
                }
            }
        }
    }

    /**
     * Finds characters to split a line of a ranking file or a judgment file
     */
    private int[] splits(String s, int columns) {
        int[] result = new int[2 * columns];
        boolean lastWs = true;
        int column = 0;
        result[0] = 0;

        for (int i = 0; i < s.length() && column < columns; i++) {
            char c = s.charAt(i);
            boolean isWs = (c == ' ') || (c == '\t');

            if (!isWs && lastWs) {
                result[2 * column] = i;
            } else if (isWs && !lastWs) {
                result[2 * column + 1] = i;
                column++;
            }

            lastWs = isWs;
        }

        return result;
    }

    private void check(ArrayList<ScoredDocument> rankedList) {
        for (ScoredDocument sdoc : rankedList) {
            assert (sdoc.rank != 0) : "Ranked list contains a document with zero rank. Ranked lists must start from 1.";
        }
    }
}
