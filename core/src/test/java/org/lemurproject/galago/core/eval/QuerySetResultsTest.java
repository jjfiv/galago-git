/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.eval;

import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author michaelz
 */
public class QuerySetResultsTest {

    List<EvalDoc> docs1 = new ArrayList<>();
    List<EvalDoc> docs2 = new ArrayList<>();

    public QuerySetResultsTest() {
    }

    @Before
    public void setUp() {

        docs1.add(new ScoredDocument("doc1-1", 1, -1.5));
        docs1.add(new ScoredDocument("doc1-2", 2, -1.6));
        docs1.add(new ScoredDocument("doc1-3", 3, -1.7));
        docs1.add(new ScoredDocument("doc1-4", 4, -1.8));

        docs2.add(new ScoredDocument("doc2-1", 1, -2.5));
        docs2.add(new ScoredDocument("doc2-2", 2, -2.6));
        docs2.add(new ScoredDocument("doc2-3", 3, -2.7));
        docs2.add(new ScoredDocument("doc2-4", 4, -2.8));
    }

    @Test
    public void testConstructor() throws IOException {

        List<EvalDoc> unmodifiable1 = Collections.unmodifiableList(docs1);
        List<EvalDoc> unmodifiable2 = Collections.unmodifiableList(docs2);

        // The constructor does a sort, the old version of the code would fail
        // on the sort if an immutable list was passed in with an  UnsupportedOperationException.
        Map<String, List<EvalDoc>> results = new HashMap<>();

        results.put("docs1", unmodifiable1);
        results.put("docs2", unmodifiable2);

        QuerySetResults rs = new QuerySetResults(results);
        assertNotNull(rs);

        File tmp = FileUtility.createTemporary();
        try {
            // create a TREC ranking file
            String qrels
                    = "1 Q0 WSJ880711-0086 39 -3.05948 Exp\n"
                    + "2 Q0 WSJ880711-0087 40 -3.15948 Exp\n";

            StreamUtil.copyStringToFile(qrels, tmp);
            QuerySetResults rs2 = new QuerySetResults(tmp.getAbsolutePath());
            assertNotNull(rs2);

        } finally {
            tmp.delete();
        }
    }

    /**
     * Test of get method, of class QuerySetResults.
     */
    @Test
    public void testGet() {

        // make a copy of the docs and shuffle them
        ArrayList<EvalDoc> newDocs = new ArrayList<>();
        for (EvalDoc d : docs1) {
            newDocs.add(new ScoredDocument(d.getName(), d.getRank(), d.getScore()));
        }

        // shuffle the new docs
        Collections.shuffle(newDocs);

        Map<String, List<EvalDoc>> results = new HashMap<>();

        results.put("query docs1", newDocs);
        QuerySetResults rs = new QuerySetResults(results);
        QueryResults newResults = rs.get("query docs1");

        int i = 0;
        for (EvalDoc sd : newResults.getIterator()) {
            assertTrue(sd.equals(docs1.get(i++)));
        }

    }

    /**
     * Test of getName method, of class QuerySetResults.
     */
    @Test
    public void testGetName() {

        Map<String, List<EvalDoc>> results = new HashMap<>();

        results.put("query docs1", docs1);
        results.put("query docs2", docs2);
        QuerySetResults rs = new QuerySetResults(results);

        assertEquals("results", rs.getName());

    }

}
