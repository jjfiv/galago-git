package org.lemurproject.galago.core.retrieval.traversal;

import java.util.Arrays;

import junit.framework.TestCase;

import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.tupleflow.Parameters;

public class Prms3TraversalTest extends TestCase {

//    public void testPrms3ModelCorrectness() throws Exception {
//        DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());
//
//        // set fields
//        Parameters global = new Parameters();
//
//        LocalRetrieval retrieval = new LocalRetrieval(index, global);
//        Parameters qp = new Parameters();
//        qp.set("fieldWeightAlpha", 1.0d);
//        
//        String[] fields = {"title", "author", "anchor"};
//        qp.set("fields", Arrays.asList(fields));
//        
//        ScoredDocument[] results = retrieval.runQuery("#prms3(cat dog donkey)", qp);
//
//        assertEquals(5, results.length);
//
//        assertEquals(1, results[0].document);
//        assertEquals(results[0].score, -11.160840, 0.00001);
//        assertEquals(2, results[1].document);
//        assertEquals(results[1].score, -11.172624, 0.00001);
//        assertEquals(5, results[2].document);
//        assertEquals(results[2].score, -11.189912, 0.00001);
//        assertEquals(4, results[3].document);
//        assertEquals(results[3].score, -11.231324, 0.00001);
//        assertEquals(3, results[4].document);
//        assertEquals(results[4].score, -11.240375, 0.00001);
//
//        Parameters weights = new Parameters();
//        weights.set("title", 0.5);
//        weights.set("author", 0.2);
//        weights.set("anchor", 0.3);
//        
//        qp.set("weights", weights);
//        
//        results = retrieval.runQuery("#prms3(cat dog donkey)", qp);
//
//        assertEquals(5, results.length);
//
//        assertEquals(1, results[0].document);
//        assertEquals(results[0].score, -11.160840, 0.00001);
//        assertEquals(2, results[1].document);
//        assertEquals(results[1].score, -11.172624, 0.00001);
//        assertEquals(5, results[2].document);
//        assertEquals(results[2].score, -11.189912, 0.00001);
//        assertEquals(4, results[3].document);
//        assertEquals(results[3].score, -11.231324, 0.00001);
//        assertEquals(3, results[4].document);
//        assertEquals(results[4].score, -11.240375, 0.00001);
//        
//        
//        qp.set("fieldWeightAlpha", 0.5d);
//        results = retrieval.runQuery("#prms3(cat dog donkey)", qp);
//
//        assertEquals(5, results.length);
//        assertEquals(1, results[0].document);
//        assertEquals(results[0].score, -11.160840, 0.00001);
//        assertEquals(2, results[1].document);
//        assertEquals(results[1].score, -11.172624, 0.00001);
//        assertEquals(5, results[2].document);
//        assertEquals(results[2].score, -11.189912, 0.00001);
//        assertEquals(4, results[3].document);
//        assertEquals(results[3].score, -11.231324, 0.00001);
//        assertEquals(3, results[4].document);
//        assertEquals(results[4].score, -11.240375, 0.00001);
//
//      }
}
