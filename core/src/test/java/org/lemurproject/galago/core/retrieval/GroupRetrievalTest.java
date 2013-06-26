/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import java.io.File;
import java.util.Arrays;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class GroupRetrievalTest extends TestCase {

  public GroupRetrievalTest(String name) {
    super(name);
  }

  public void testGroupRetrieval() throws Exception {
    File trecCorpusFile1 = null;
    File trecCorpusFile2 = null;
    File index1 = null;
    File index2 = null;

    try {
      // create index 1
      String trecCorpus = AppTest.trecDocument("i1-55", "This is a sample document")
              + AppTest.trecDocument("i1-59", "sample document two");

      trecCorpusFile1 = Utility.createTemporary();
      Utility.copyStringToFile(trecCorpus, trecCorpusFile1);

      index1 = Utility.createTemporaryDirectory();
      App.main(new String[]{"build", "--indexPath=" + index1.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile1.getAbsolutePath()});
      AppTest.verifyIndexStructures(index1.getAbsoluteFile());

      // create index 2
      trecCorpus = AppTest.trecDocument("i2-55", "This is a sample also a document")
              + AppTest.trecDocument("i2-59", "sample document four long");

      trecCorpusFile2 = Utility.createTemporary();
      Utility.copyStringToFile(trecCorpus, trecCorpusFile2);

      index2 = Utility.createTemporaryDirectory();
      App.main(new String[]{"build", "--indexPath=" + index2.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile2.getAbsolutePath()});
      AppTest.verifyIndexStructures(index2.getAbsoluteFile());

      Parameters params = new Parameters();
      params.set("defaultGroup", "group1");
      params.set("index", new Parameters());
      String[] indexes = {index1.getAbsolutePath(), index2.getAbsolutePath()};
      params.getMap("index").set("group1", index1.getAbsolutePath());
      params.getMap("index").set("group2", Arrays.asList(indexes));

      GroupRetrieval gr = (GroupRetrieval) RetrievalFactory.instance(params);

      String query = "#combine( sample document )";
      Node parsedQuery = StructuredQuery.parse(query);

      Parameters q1 = params.clone();
      Node queryTree1 = gr.transformQuery(parsedQuery, q1, "group1");

      String expected = "#combine:w=1.0( #dirichlet:"
              + "collectionLength=8:"
              + "documentCount=2:"
              + "maximumCount=1:"
              + "nodeFrequency=2:"
              + "w=0.5("
              + " #lengths:document:part=lengths() #counts:sample:part=postings.porter() )"
              + " #dirichlet:"
              + "collectionLength=8:"
              + "documentCount=2:"
              + "maximumCount=1:"
              + "nodeFrequency=2:"
              + "w=0.5("
              + " #lengths:document:part=lengths() #counts:document:part=postings.porter() ) )";

      assertEquals(expected, queryTree1.toString());
      ScoredDocument[] res1 = gr.runQuery(queryTree1, q1, "group1");
      String[] expectedIds = { "i1-59", "i1-55" };
      double[] expectedScores = { -1.38562924636308, -1.3869590337930815 }; 

      for (int i = 0; i < res1.length; i++) {
        assertEquals(expectedIds[i], res1[i].documentName);
        assertEquals(i+1, res1[i].rank);
        assertEquals(expectedScores[i], res1[i].score, 0.00000001);
      }

      Parameters q2 = params.clone();
      Node queryTree2 = gr.transformQuery(parsedQuery, q2, "group2");

      expected = "#combine:w=1.0("
              + " #dirichlet:"
              + "collectionLength=19:"
              + "documentCount=4:"
              + "maximumCount=1:"
              + "nodeFrequency=4:"
              + "w=0.5"
              + "( #lengths:document:part=lengths() #counts:sample:part=postings.porter() ) "
              + "#dirichlet:"
              + "collectionLength=19:"
              + "documentCount=4:"
              + "maximumCount=1:"
              + "nodeFrequency=4:"
              + "w=0.5"
              + "( #lengths:document:part=lengths() #counts:document:part=postings.porter() ) )";

      assertEquals(expected, queryTree2.toString());
      ScoredDocument[] res2 = gr.runQuery(queryTree2, q2, "group2");
      expectedIds = new String[]{"i1-59", "i2-59", "i1-55", "i2-55"};
      expectedScores = new double[]{
        -1.5569809573716442,
        -1.5576460721284549, 
        -1.5583107448016458,
        -1.5596387662451652
      };
      for (int i = 0; i < res2.length; i++) {
        assertEquals(expectedIds[i], res2[i].documentName);
        assertEquals(i+1, res2[i].rank);
        assertEquals(expectedScores[i], res2[i].score, 0.00000001);
      }
    } finally {

      if (trecCorpusFile1 != null) {
        trecCorpusFile1.delete();
      }
      if (trecCorpusFile2 != null) {
        trecCorpusFile2.delete();
      }
      if (index1 != null) {
        Utility.deleteDirectory(index1);
      }
      if (index2 != null) {
        Utility.deleteDirectory(index2);
      }
    }
  }
}
