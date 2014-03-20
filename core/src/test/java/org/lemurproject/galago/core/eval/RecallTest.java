
package org.lemurproject.galago.core.eval;

import java.io.File;
import java.io.IOException;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.lemurproject.galago.core.eval.metric.Precision;
import org.lemurproject.galago.core.eval.metric.Recall;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Test the recall class
 * @author wem
 */
@RunWith(JUnit4.class)
public class RecallTest {
    @Test
    public void calculatePrecision() throws IOException {
        File tmpQ = FileUtility.createTemporary();
        File tmpR = FileUtility.createTemporary();

        try{
            //there are 10 documents total
            //query 1 has 10 relevant documents
            //query 2 has 5 relevant documents
            //query 3 has 5 relevant documents
            String qrels =
                    "1 0 doc0 1\n" +
                            "1 0 doc1 1\n" +
                            "1 0 doc2 1\n" +
                            "1 0 doc3 1\n" +
                            "1 0 doc4 1\n" +
                            "2 0 doc0 0\n" +
                            "2 0 doc1 0\n" +
                            "2 0 doc2 0\n" +
                            "2 0 doc3 0\n" +
                            "2 0 doc4 0\n" +
                            "2 0 doc5 1\n" +
                            "2 0 doc6 1\n" +
                            "2 0 doc7 1\n" +
                            "2 0 doc8 1\n" +
                            "2 0 doc9 1\n" +
                            "3 0 doc0 1\n" +
                            "3 0 doc1 1\n" +
                            "3 0 doc2 1\n" +
                            "3 0 doc3 1\n" +
                            "3 0 doc4 1\n" +
                            "3 0 doc5 0\n" +
                            "3 0 doc6 0\n" +
                            "3 0 doc7 0\n" +
                            "3 0 doc8 0\n" +
                            "3 0 doc9 0\n" +
                            "4 0 doc0 1\n" +
                            "4 0 doc1 1\n" +
                            "4 0 doc2 0\n" +
                            "4 0 doc3 0\n" +
                            "4 0 doc4 1\n" +
                            "4 0 doc5 1\n" +
                            "4 0 doc6 0\n" +
                            "4 0 doc7 0\n" +
                            "4 0 doc8 1\n" +
                            "4 0 doc9 1\n";
            Utility.copyStringToFile(qrels, tmpQ);
            QuerySetJudgments qsj = new QuerySetJudgments(tmpQ.getAbsolutePath(), true, true);
            String results =
                    "1 Q0 doc0 1 -10.08 galago\n" +
                            "1 Q0 doc1 2 -10.12 galago\n" +
                            "1 Q0 doc2 3 -11.08 galago\n" +
                            "1 Q0 doc3 4 -11.55 galago\n" +
                            "1 Q0 doc4 5 -11.85 galago\n" +
                            "1 Q0 doc5 6 -12.08 galago\n" +
                            "1 Q0 doc6 7 -12.12 galago\n" +
                            "1 Q0 doc7 8 -13.08 galago\n" +
                            "1 Q0 doc8 9 -13.55 galago\n" +
                            "1 Q0 doc9 10 -13.85 galago\n" +
                            "2 Q0 doc0 1 -10.08 galago\n" +
                            "2 Q0 doc1 2 -10.12 galago\n" +
                            "2 Q0 doc2 3 -11.08 galago\n" +
                            "2 Q0 doc3 4 -11.55 galago\n" +
                            "2 Q0 doc4 5 -11.85 galago\n" +
                            "2 Q0 doc5 6 -12.08 galago\n" +
                            "2 Q0 doc6 7 -12.12 galago\n" +
                            "2 Q0 doc7 8 -13.08 galago\n" +
                            "2 Q0 doc8 9 -13.55 galago\n" +
                            "2 Q0 doc9 10 -13.85 galago\n" +
                            "3 Q0 doc0 1 -10.08 galago\n" +
                            "3 Q0 doc1 2 -10.12 galago\n" +
                            "3 Q0 doc2 3 -11.08 galago\n" +
                            "3 Q0 doc3 4 -11.55 galago\n" +
                            "3 Q0 doc4 5 -11.85 galago\n" +
                            "3 Q0 doc5 6 -12.08 galago\n" +
                            "3 Q0 doc6 7 -12.12 galago\n" +
                            "3 Q0 doc7 8 -13.08 galago\n" +
                            "3 Q0 doc8 9 -13.55 galago\n" +
                            "3 Q0 doc9 10 -13.85 galago\n" +
                            "4 Q0 doc0 1 -10.08 galago\n" +
                            "4 Q0 doc1 2 -10.12 galago\n" +
                            "4 Q0 doc2 3 -11.08 galago\n" +
                            "4 Q0 doc3 4 -11.55 galago\n" +
                            "4 Q0 doc4 5 -11.85 galago\n" +
                            "4 Q0 doc5 6 -12.08 galago\n" +
                            "4 Q0 doc6 7 -12.12 galago\n" +
                            "4 Q0 doc7 8 -13.08 galago\n" +
                            "4 Q0 doc8 9 -13.55 galago\n" +
                            "4 Q0 doc9 10 -13.85 galago\n";
            Utility.copyStringToFile(results, tmpR);
            QuerySetResults qsr = new QuerySetResults(tmpR.getAbsolutePath());
            Precision rFive = new Precision(5);
            Precision rTen = new Precision(10);
            assertEquals(1.000, rFive.evaluate(qsr.get("1"), qsj.get("1")),.0001);
            assertEquals(0.000, rFive.evaluate(qsr.get("2"), qsj.get("2")),.0001);
            assertEquals(1.000, rFive.evaluate(qsr.get("3"), qsj.get("3")),.0001);
            assertEquals(0.500, rFive.evaluate(qsr.get("4"), qsj.get("4")),.0001);
            assertEquals(1.000, rTen.evaluate(qsr.get("1"), qsj.get("1")),.0001);
            assertEquals(1.000, rTen.evaluate(qsr.get("2"), qsj.get("2")),.0001);
            assertEquals(1.000, rTen.evaluate(qsr.get("3"), qsj.get("3")),.0001);
            assertEquals(1.000, rTen.evaluate(qsr.get("4"), qsj.get("4")),.0001);
        }
        finally{
            tmpQ.delete();
            tmpR.delete();
        }
    }
}
