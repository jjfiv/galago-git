package org.lemurproject.galago.core.retrieval.processing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamUtil;

/**
 *
 * @author michaelz copying much of RankedPassageModelTest
 */
public class MaxPassageFinderTest {

    static File corpus = null;
    static File index = null;

    @BeforeClass
    public static void setUp() throws Exception {
        corpus = FileUtility.createTemporary();
        index = FileUtility.createTemporaryDirectory();
        makeIndex(corpus, index);
    }

    @AfterClass
    public static void tearDown() throws IOException {
        if (corpus != null) {
            corpus.delete();
        }
        if (index != null) {
            FSUtil.deleteDirectory(index);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingPassageQuery() throws Exception {
        Parameters badParms = Parameters.create();
        badParms.set("requested", 100);
        badParms.set("passageSize", 10);
        badParms.set("passageShift", 5);
        badParms.set("working", Arrays.asList(new Long[]{2l}));
        LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath());
        Node query = StructuredQuery.parse("test");
        query = ret.transformQuery(query, badParms);
        MaxPassageFinder model = new MaxPassageFinder(ret);
        model.execute(query, badParms);
    }

    @Test
    public void testEntireCollection() throws Exception {
        Parameters globals = Parameters.create();
        globals.set("passageQuery", true);
        LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

        Parameters queryParams = Parameters.create();

        queryParams.set("passageQuery", true);
        queryParams.set("passageSize", 10);
        queryParams.set("passageShift", 5);
        List docs = new ArrayList<Long>();
        for (int i = 0; i < 100; i++) {
            docs.add(new Long(i));
        }
        queryParams.set("working", docs);
        Node query = StructuredQuery.parse("#combine( test text 0 1 )");
        query = ret.transformQuery(query, queryParams);
        // List<ScoredDocument> results = ret.executeQuery(query, Parameters.create()).scoredDocuments;

        MaxPassageFinder model = new MaxPassageFinder(ret);

        ScoredPassage[] results = (ScoredPassage[]) model.execute(query, queryParams);

        // --- all documents contain these terms in the first ten words --
        // -> this query should only ever return the first passage (0-10)
        // -> and the top 100 scores should be equal
        assertEquals(results.length, 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(results[i].document, i);
            assertEquals(results[i].begin, 0);
            assertEquals(results[i].end, 10);
            if (i > 0) {
                assert (CmpUtil.compare(results[i].score, results[i - 1].score) == 0);
            }
        }

        // search for something that doesn't exist - this is a test for
        // https://sourceforge.net/p/lemur/feature-requests/115/
        // which used to crash if the query was not found in the document(s)
        query = StructuredQuery.parse("michael zarozinski");
        query = ret.transformQuery(query, queryParams);
        results = (ScoredPassage[]) model.execute(query, queryParams);

        // only look in 3 documents, so we should only get 3 results. 
        // If we were doing RankedPassageModel retrieval we would get
        // up to as many as we specify in the "requested" parameter.
        queryParams.set("working",
                Arrays.asList(new String[]{"d-1", "d-98", "d-99"}));
        //there is one doc with the term '108' so that doc (99)
        // should have the highest ranking.

        query = StructuredQuery.parse("#combine( test text 108 )");
        query = ret.transformQuery(query, queryParams);

        results = (ScoredPassage[]) model.execute(query, queryParams);

        assertEquals(results.length, 3);
        assertEquals(results[0].document, 99);

    }

    private static void makeIndex(File corpus, File index) throws Exception {
        StringBuilder c = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            StringBuilder data = new StringBuilder();
            for (int j = 0; j < (i + 10); j++) {
                data.append(" ").append(j);
            }
            c.append(AppTest.trecDocument("d-" + i, "Test text" + data.toString()));
        }
        StreamUtil.copyStringToFile(c.toString(), corpus);

        Parameters p = Parameters.create();
        p.set("inputPath", corpus.getAbsolutePath());
        p.set("indexPath", index.getAbsolutePath());
        p.set("corpus", false);
        App.run("build", p, System.out);
    }
}
