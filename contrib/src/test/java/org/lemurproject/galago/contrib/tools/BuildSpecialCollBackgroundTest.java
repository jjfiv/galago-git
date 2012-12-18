/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import java.io.File;
import java.util.Random;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class BuildSpecialCollBackgroundTest extends TestCase {

  public BuildSpecialCollBackgroundTest(String testName) {
    super(testName);
  }

  public void testSomeMethod() throws Exception {
    File docs = Utility.createTemporary();
    File back = Utility.createTemporary();
    File index = Utility.createTemporaryDirectory();
    try {
      makeTrecDocs(docs);

      Parameters p = new Parameters();
      p.set("inputPath", docs.getAbsolutePath());
      p.set("indexPath", index.getAbsolutePath());
      p.set("corpus", false);
      App.run("build", p, System.out);

      StringBuilder sb = new StringBuilder();
      sb.append("1\t1\n");
      sb.append("2\t2\n");
      sb.append("3\t3\n");
      sb.append("4\t4\n");
      sb.append("4\t4\n");
      Utility.copyStringToFile(sb.toString(), back);

      Parameters p2 = new Parameters();
      p2.set("inputPath", back.getAbsolutePath());
      p2.set("indexPath", index.getAbsolutePath());
      p2.set("partName", "back");
      App.run("build-special-coll-background", p2, System.out);

      for(File f : index.listFiles()){
        System.err.println(f.getAbsolutePath());
      }
      
      Retrieval r = RetrievalFactory.instance(index.getAbsolutePath(), new Parameters());
      System.err.println(r.getAvailableParts().toPrettyString());
      
    } finally {
      docs.delete();
      back.delete();
      Utility.deleteDirectory(index);
    }
  }

  private void makeTrecDocs(File input) throws Exception {
    Random r = new Random();
    StringBuilder corpus = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      StringBuilder text = new StringBuilder();
      for (int j = 0; j < 100; j++) {
        text.append(" ").append(r.nextInt(100));
//        text.append(" ").append(j);
      }
      corpus.append(trecDocument("doc-" + i, text.toString()));
    }
    Utility.copyStringToFile(corpus.toString(), input);
  }

  public static String trecDocument(String docno, String text) {
    return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n"
            + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
  }
}
