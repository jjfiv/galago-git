/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.eval;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author michaelz
 */
public class QuerySetResultsTest {

    List<ScoredDocument> docs1 = new ArrayList<ScoredDocument>();
    List<ScoredDocument> docs2 = new ArrayList<ScoredDocument>();

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

        List<ScoredDocument> unmodifiable1 = Collections.unmodifiableList(docs1);
        List<ScoredDocument> unmodifiable2 = Collections.unmodifiableList(docs2);

        // The constructor does a sort, the old version of the code would fail
        // on the sort if an immutable list was passed in with an  UnsupportedOperationException.
        Map<String, List<ScoredDocument>> results = new HashMap<String, List<ScoredDocument>>();

        results.put("docs1", unmodifiable1);
        results.put("docs2", unmodifiable2);

        QuerySetResults rs = new QuerySetResults(results);

        File tmp = FileUtility.createTemporary();
        try {
            // create a TREC ranking file
            String qrels
                    = "1 Q0 WSJ880711-0086 39 -3.05948 Exp\n"
                    + "2 Q0 WSJ880711-0087 40 -3.15948 Exp\n";

            Utility.copyStringToFile(qrels, tmp);
            QuerySetResults rs2 = new QuerySetResults(tmp.getAbsolutePath());

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
        ArrayList<ScoredDocument> newDocs = new ArrayList<ScoredDocument>();
        for (ScoredDocument d : docs1) {
            newDocs.add(d.clone(d.score));
        }

        // shuffle the new docs
        Collections.shuffle(newDocs);

        Map<String, List<ScoredDocument>> results = new HashMap<String, List<ScoredDocument>>();

        results.put("query docs1", newDocs);
        QuerySetResults rs = new QuerySetResults(results);
        QueryResults newResults = rs.get("query docs1");

        int i = 0;
        for (ScoredDocument sd : newResults.getIterator()) {
            assertTrue(sd.equals(docs1.get(i++)));
        }

    }

    /**
     * Test of getName method, of class QuerySetResults.
     */
    @Test
    public void testGetName() {

        Map<String, List<ScoredDocument>> results = new HashMap<String, List<ScoredDocument>>();

        results.put("query docs1", docs1);
        results.put("query docs2", docs2);
        QuerySetResults rs = new QuerySetResults(results);

        assertEquals("results", rs.getName());

    }

}
