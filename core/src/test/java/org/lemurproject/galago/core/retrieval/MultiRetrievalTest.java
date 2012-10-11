/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.retrieval.MultiRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
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
public class MultiRetrievalTest extends TestCase {

  public MultiRetrievalTest(String name) {
    super(name);
  }

  public void testMultiRetrieval() throws Exception {
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

      // create index 2
      trecCorpus = AppTest.trecDocument("i2-55", "This is a sample also a document")
              + AppTest.trecDocument("i2-59", "sample document four long");

      trecCorpusFile2 = Utility.createTemporary();
      Utility.copyStringToFile(trecCorpus, trecCorpusFile2);

      index2 = Utility.createTemporaryDirectory();
      App.main(new String[]{"build", "--indexPath=" + index2.getAbsolutePath(),
                "--inputPath=" + trecCorpusFile2.getAbsolutePath()});

      Parameters params = new Parameters();
      String[] indexes = {index1.getAbsolutePath(), index2.getAbsolutePath()};
      params.set("index", Arrays.asList(indexes));
      MultiRetrieval mr = (MultiRetrieval) RetrievalFactory.instance(params);
      String query = "#combine( sample document )";
      Node parsedQuery = StructuredQuery.parse(query);
      Parameters qp = new Parameters();
      Node queryTree = mr.transformQuery(parsedQuery, qp);

      String expected = "#combine("
              + " #feature:dirichlet:"
              + "collectionLength=19:"
              + "collectionProbability=0.21052631578947367:"
              + "documentCount=4:"
              + "w=0.5"
              + "( #counts:sample:part=postings.porter() ) "
              + "#feature:dirichlet:"
              + "collectionLength=19:"
              + "collectionProbability=0.21052631578947367:"
              + "documentCount=4:"
              + "w=0.5"
              + "( #counts:document:part=postings.porter() ) )";

      assertEquals(queryTree.toString(), expected);

      ScoredDocument[] res = mr.runQuery(queryTree, qp);

      ScoredDocument[] expDocs = new ScoredDocument[4];
      expDocs[0] = new ScoredDocument("i1-59", 1, -1.5569809573716442);
      expDocs[1] = new ScoredDocument("i2-59", 2, -1.5576460721284549);
      expDocs[2] = new ScoredDocument("i1-55", 3, -1.5583107448016458);
      expDocs[3] = new ScoredDocument("i2-55", 4, -1.5596387662451652);

      assertEquals(expDocs.length, res.length);
      for (int i = 0; i < res.length; i++) {
        assertEquals(expDocs[i].documentName, res[i].documentName);
        assertEquals(expDocs[i].rank, res[i].rank);
        assertEquals(expDocs[i].score, res[i].score, 0.000001);
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
