
package org.lemurproject.galago.core.eval;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test some simple Qrel behavior.
 * 
 * @author jfoley
 */
public class QrelTest {
  @Test
  public void loadAndVerifyQrel() throws IOException {
    File tmp = FileUtility.createTemporary();
    try {
      String qrels = 
              "1 0 doc0 1\n" +
              "1 0 doc1 0\n" +
              "2 0 doc2 1\n" +
              "2 0 doc3 1\n";

      Utility.copyStringToFile(qrels, tmp);
      QuerySetJudgments qsj = new QuerySetJudgments(tmp.getAbsolutePath(), true, true);
      
      assertEquals(qsj.size(), 2);
      assertTrue(qsj.containsKey("1"));
      assertTrue(qsj.containsKey("2"));
      
      QueryJudgments qid1 = qsj.get("1");
      assertEquals(qid1.getRelevantJudgmentCount(), 1);
      assertEquals(qid1.getNonRelevantJudgmentCount(), 1);

      QueryJudgments qid2 = qsj.get("2");
      assertEquals(qid2.getRelevantJudgmentCount(), 2);
      assertEquals(qid2.getNonRelevantJudgmentCount(), 0);

      // doc0 is R, doc1 is NR
      assertEquals(qid1.isRelevant("doc0"), true);
      assertEquals(qid1.isNonRelevant("doc0"), false);
      assertEquals(qid1.isRelevant("doc1"), false);
      assertEquals(qid1.isNonRelevant("doc1"), true);
      
      // since doc2, doc3 are unjduged they are neither relevent nor non-rel
      assertEquals(qid1.isNonRelevant("doc2"), false);
      assertEquals(qid1.isNonRelevant("doc3"), false);
      assertEquals(qid1.isRelevant("doc2"), false);
      assertEquals(qid1.isRelevant("doc3"), false);
      
    } finally {
      tmp.delete();
    }
  }

  @Test
  public void saveAndLoadQrel() throws IOException {
    File tmp = FileUtility.createTemporary();
    File tmp2 = FileUtility.createTemporary();

    try {
      String qrels =
        "1 0 doc0 1\n" +
          "1 0 doc1 0\n" +
          "2 0 doc2 1\n" +
          "2 0 doc3 1\n";

      Utility.copyStringToFile(qrels, tmp);
      QuerySetJudgments qsj = new QuerySetJudgments(tmp.getAbsolutePath(), true, true);

      assertEquals(qsj.size(), 2);
      assertTrue(qsj.containsKey("1"));
      assertTrue(qsj.containsKey("2"));

      QueryJudgments qid1 = qsj.get("1");
      assertEquals(qid1.getRelevantJudgmentCount(), 1);
      assertEquals(qid1.getNonRelevantJudgmentCount(), 1);

      QueryJudgments qid2 = qsj.get("2");
      assertEquals(qid2.getRelevantJudgmentCount(), 2);
      assertEquals(qid2.getNonRelevantJudgmentCount(), 0);

      // doc0 is R, doc1 is NR
      assertEquals(qid1.isRelevant("doc0"), true);
      assertEquals(qid1.isNonRelevant("doc0"), false);
      assertEquals(qid1.isRelevant("doc1"), false);
      assertEquals(qid1.isNonRelevant("doc1"), true);

      // since doc2, doc3 are unjduged they are neither relevant nor non-rel
      assertEquals(qid1.isNonRelevant("doc2"), false);
      assertEquals(qid1.isNonRelevant("doc3"), false);
      assertEquals(qid1.isRelevant("doc2"), false);
      assertEquals(qid1.isRelevant("doc3"), false);

      qsj.saveJudgments(tmp2);

      QuerySetJudgments qsj2  = new QuerySetJudgments(tmp2.getAbsolutePath(), true, true);

      assertEquals(qsj.keySet(), qsj2.keySet());

    } finally {
      tmp.delete();
      tmp2.delete();
    }

  }
}
