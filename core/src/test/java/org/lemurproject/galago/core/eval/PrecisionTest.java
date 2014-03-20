
package org.lemurproject.galago.core.eval;

import java.io.File;
import java.io.IOException;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.lemurproject.galago.core.eval.metric.Precision;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Test the precision class
 * Checks the following calculations:
 * Precision when all retrieved documents are relevant
 * Precision when some retrieved documents are relevant
 * Precision when some documents are not scored for a query
 * Precision when no retrieved documents are relevant
 * @author wem
 */
@RunWith(JUnit4.class)
public class PrecisionTest {
    @Test
    public void calculatePrecision() throws IOException {
        File tmpQ = FileUtility.createTemporary();
        File tmpR = FileUtility.createTemporary();

    try{
        String qrels =
                "1 0 doc0 1\n" +
                "1 0 doc1 1\n" +
                "1 0 doc2 1\n" +
                "1 0 doc3 1\n" +
                "2 0 doc0 0\n" +
                "2 0 doc1 1\n" +
                "2 0 doc2 1\n" +
                "2 0 doc3 1\n" +
                "3 0 doc1 1\n" +
                "3 0 doc2 1\n" +
                "3 0 doc3 1\n" +
                "4 0 doc1 0\n";

        Utility.copyStringToFile(qrels, tmpQ);
        QuerySetJudgments qsj = new QuerySetJudgments(tmpQ.getAbsolutePath(), true, true);
        String results =
                "1 Q0 doc1 1 -12.08 galago\n" +
                "1 Q0 doc2 2 -12.12 galago\n" +
                "1 Q0 doc3 3 -13.08 galago\n" +
                "1 Q0 doc0 4 -13.55 galago\n" +
                "2 Q0 doc1 1 -12.08 galago\n" +
                "2 Q0 doc2 2 -12.12 galago\n" +
                "2 Q0 doc3 3 -13.08 galago\n" +
                "2 Q0 doc0 4 -13.55 galago\n" +
                "3 Q0 doc1 1 -12.08 galago\n" +
                "3 Q0 doc2 2 -12.12 galago\n" +
                "3 Q0 doc3 3 -13.08 galago\n" +
                "3 Q0 doc0 4 -13.55 galago\n" +
                "4 Q0 doc1 1 -12.08 galago\n" +
                "4 Q0 doc2 2 -12.12 galago\n" +
                "4 Q0 doc3 3 -13.08 galago\n" +
                "4 Q0 doc0 4 -13.55 galago\n";
        Utility.copyStringToFile(results, tmpR);
        QuerySetResults qsr = new QuerySetResults(tmpR.getAbsolutePath());
        Precision p = new Precision(4);
        assertEquals(1.00, p.evaluate(qsr.get("1"), qsj.get("1")),.0001);
        assertEquals(0.75, p.evaluate(qsr.get("2"), qsj.get("2")),.0001);
        assertEquals(0.75, p.evaluate(qsr.get("3"), qsj.get("3")),.0001);
        assertEquals(0.00, p.evaluate(qsr.get("4"), qsj.get("4")),.0001);
        assertEquals(1.00, (new Precision(3)).evaluate(qsr.get("2"), qsj.get("2")),.0001);
    }
        finally{
            tmpQ.delete();
            tmpR.delete();
        }
    }
}
