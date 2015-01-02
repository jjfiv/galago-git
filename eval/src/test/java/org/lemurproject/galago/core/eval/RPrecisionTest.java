
package org.lemurproject.galago.core.eval;

import org.junit.Assert;
import org.junit.Test;
import org.lemurproject.galago.core.eval.metric.RPrecision;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.File;
import java.io.IOException;

/**
 * Test the RPrecision class
 * @author wem
 */
public class RPrecisionTest {
  @Test
  public void calculatePrecision() throws IOException {
    File tmpQ = File.createTempFile("tmpQ", "");
    File tmpR = File.createTempFile("tmpR", "");

    try{
      //there are 10 documents total
      //query 1 has 10 relevant documents
      //query 2 has 5 relevant documents
      //query 3 has 5 relevant documents
      //query 4 has 6 relevant documents
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
      StreamUtil.copyStringToFile(qrels, tmpQ);
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
      StreamUtil.copyStringToFile(results, tmpR);
      QuerySetResults qsr = new QuerySetResults(tmpR.getAbsolutePath());
      RPrecision rp = new RPrecision("");
      Assert.assertEquals(1.000, rp.evaluate(qsr.get("1"), qsj.get("1")), .001);
      Assert.assertEquals(0.000, rp.evaluate(qsr.get("2"), qsj.get("2")), .001);
      Assert.assertEquals(1.000, rp.evaluate(qsr.get("3"), qsj.get("3")), .001);
      Assert.assertEquals(0.667, rp.evaluate(qsr.get("4"), qsj.get("4")), .001);
    }
    finally{
      tmpQ.delete();
      tmpR.delete();
    }
  }
}
