/*
 * BSD License (http://lemurproject.org/galago-license)

 */
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
public class PassageRetrievalTest extends TestCase {

  File trecCorpusFile, corpusFile, indexFile;

  public PassageRetrievalTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws Exception {
    File[] files = LocalRetrievalTest.make10DocIndex();
    trecCorpusFile = files[0];
    corpusFile = files[1];
    indexFile = files[2];
  }

  @Override
  public void tearDown() throws IOException {
    trecCorpusFile.delete();
    Utility.deleteDirectory(indexFile);
    Utility.deleteDirectory(corpusFile);
  }

  public void testPassageRetrieval() throws Exception {
    Parameters p = new Parameters();
    p.set("passageSize", 4);
    p.set("passageShift", 2);

    LocalRetrieval retrieval = new LocalRetrieval(indexFile.toString(), p);

    String query = "#combine( cat document )";
    Node root = StructuredQuery.parse(query);
    root = retrieval.transformQuery(root);

    p.set("requested", 10);
    ScoredPassage[] result = (ScoredPassage[]) retrieval.runQuery(root, p);
    assertEquals(6, result.length);
    assert (result[0].documentName.equals("9") && result[0].begin == 0 && result[0].end == 4);
    assert (result[1].documentName.equals("8") && result[1].begin == 2 && result[1].end == 6);
    assert (result[2].documentName.equals("8") && result[2].begin == 0 && result[2].end == 4);
    assert (result[3].documentName.equals("1") && result[3].begin == 0 && result[3].end == 4);
    assert (result[4].documentName.equals("2") && result[4].begin == 2 && result[4].end == 6);
    assert (result[5].documentName.equals("2") && result[5].begin == 0 && result[5].end == 4);
    assertEquals(result[0].score, -2.623394, 0.001);
    assertEquals(result[1].score, -2.624723, 0.001);
    assertEquals(result[2].score, -2.624723, 0.001);
    assertEquals(result[3].score, -2.637740, 0.001);
    assertEquals(result[4].score, -2.638404, 0.001);
    assertEquals(result[5].score, -2.638404, 0.001);
  }
}
